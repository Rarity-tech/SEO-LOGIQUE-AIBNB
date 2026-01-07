#!/usr/bin/env python3
"""
=============================================================================
AIRBNB SEO RESEARCH - Analyse des facteurs de positionnement
=============================================================================
Collecte les données de position des listings selon différents scénarios
de dates pour comprendre l'algorithme de ranking Airbnb.

Zone fixe : Downtown Dubai (placeId)
Variables : jours avant check-in (60, 30, 15, 10, 5) × durée (3, 5 nuits)

NOUVELLES VARIABLES AJOUTÉES :
- instant_book : Réservation instantanée activée
- cancellation_policy : Politique d'annulation
- has_pool : Piscine disponible
- has_gym : Salle de sport disponible
- has_parking : Parking disponible
- amenities_count : Nombre total d'équipements
- min_nights : Nombre minimum de nuits
- max_nights : Nombre maximum de nuits
=============================================================================
"""

import os
import csv
import time
import json
import re
import base64
from datetime import datetime, timedelta
from curl_cffi import requests as curl_requests
import pyairbnb

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Zone fixe : Downtown Dubai
PLACE_ID = "ChIJg_kMcC9oXz4RBLnAdrBYzLU"
QUERY = "Downtown Dubai"

# Paramètres depuis environnement (modifiables dans le workflow)
GUESTS = int(os.environ.get("GUESTS", "2"))
CURRENCY = os.environ.get("CURRENCY", "AED")

# Scénarios à exécuter
RUN_ALL = os.environ.get("RUN_ALL", "true").lower() == "true"

# ==============================================================================
# SCÉNARIOS PRÉ-CONFIGURÉS
# ==============================================================================
# Format: (run_id, days_from_now, nights, env_var_name)

SCENARIOS = [
    ("60J-3N", 60, 3, "RUN_60J_3N"),
    ("60J-5N", 60, 5, "RUN_60J_5N"),
    ("30J-3N", 30, 3, "RUN_30J_3N"),
    ("30J-5N", 30, 5, "RUN_30J_5N"),
    ("15J-3N", 15, 3, "RUN_15J_3N"),
    ("15J-5N", 15, 5, "RUN_15J_5N"),
    ("10J-3N", 10, 3, "RUN_10J_3N"),
    ("10J-5N", 10, 5, "RUN_10J_5N"),
    ("5J-3N", 5, 3, "RUN_5J_3N"),
    ("5J-5N", 5, 5, "RUN_5J_5N"),
]

# ==============================================================================
# POUR MODIFIER LES PARAMÈTRES FACILEMENT :
# ==============================================================================
# 
# 1. CHANGER LE NOMBRE DE VOYAGEURS :
#    → Modifier GUESTS ci-dessus ou dans le workflow GitHub
#
# 2. CHANGER LA ZONE :
#    → Remplacer PLACE_ID et QUERY ci-dessus
#    → Pour trouver un placeId : chercher sur Airbnb, copier depuis l'URL
#
# 3. AJOUTER/MODIFIER DES SCÉNARIOS :
#    → Ajouter/modifier dans la liste SCENARIOS ci-dessus
#    → Format: ("ID", jours_avant, nuits, "ENV_VAR")
#
# ==============================================================================

# Constantes API
AIRBNB_API_KEY = "d306zoyjsyarp7ifhu67rjxn52tv0t20"
GRAPHQL_HASH = "d9ab2c7e443b50fdce5cdcb69d4f7e7626dbab1609c981565a6c4bdbb04546e3"
DELAY_BETWEEN_PAGES = 1.0
DELAY_BETWEEN_DETAILS = 0.8
DELAY_BETWEEN_SCENARIOS = 5.0
MAX_PAGES = 15  # 15 pages × 18 listings = 270 max


# ==============================================================================
# RECHERCHE API GraphQL v3
# ==============================================================================

def search_listings(check_in, check_out, guests):
    """
    Recherche Airbnb avec l'API GraphQL v3 StaysSearch.
    Utilise placeId pour une zone stable.
    RESPECTE L'ORDRE DE POSITION.
    """
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "X-Airbnb-API-Key": AIRBNB_API_KEY,
        "X-Airbnb-GraphQL-Platform": "web",
        "X-Airbnb-GraphQL-Platform-Client": "minimalist-niobe",
        "X-CSRF-Without-Token": "1",
        "Origin": "https://www.airbnb.com",
        "Referer": f"https://www.airbnb.com/s/{QUERY}/homes",
    }
    
    def build_raw_params(cursor=None):
        """Construit les rawParams pour la requête GraphQL."""
        params = [
            {"filterName": "cdnCacheSafe", "filterValues": ["false"]},
            {"filterName": "channel", "filterValues": ["EXPLORE"]},
            {"filterName": "checkin", "filterValues": [check_in]},
            {"filterName": "checkout", "filterValues": [check_out]},
            {"filterName": "datePickerType", "filterValues": ["calendar"]},
            {"filterName": "flexibleTripLengths", "filterValues": ["one_week"]},
            {"filterName": "itemsPerGrid", "filterValues": ["18"]},  # 18 par page comme Airbnb
            {"filterName": "placeId", "filterValues": [PLACE_ID]},
            {"filterName": "query", "filterValues": [QUERY]},
            {"filterName": "refinementPaths", "filterValues": ["/homes"]},
            {"filterName": "screenSize", "filterValues": ["large"]},
            {"filterName": "searchMode", "filterValues": ["regular_search"]},
            {"filterName": "tabId", "filterValues": ["home_tab"]},
            {"filterName": "version", "filterValues": ["1.8.3"]},
        ]
        
        # Nombre de voyageurs
        if guests:
            params.append({"filterName": "adults", "filterValues": [str(guests)]})
        
        # Pagination
        if cursor:
            params.append({"filterName": "cursor", "filterValues": [cursor]})
        
        return params
    
    def build_graphql_payload(cursor=None):
        """Construit le payload complet pour la requête GraphQL."""
        raw_params = build_raw_params(cursor)
        
        treatment_flags = [
            "feed_map_decouple_m11_treatment",
            "recommended_amenities_2024_treatment_b",
            "filter_redesign_2024_treatment",
            "filter_reordering_2024_roomtype_treatment",
            "p2_category_bar_removal_treatment",
            "selected_filters_2024_treatment",
            "recommended_filters_2024_treatment_b",
            "m13_search_input_phase2_treatment",
            "m13_search_input_services_enabled"
        ]
        
        search_request = {
            "metadataOnly": False,
            "requestedPageType": "STAYS_SEARCH",
            "searchType": "filter_change",
            "treatmentFlags": treatment_flags,
            "maxMapItems": 9999,
            "rawParams": raw_params
        }
        
        return {
            "operationName": "StaysSearch",
            "variables": {
                "staysSearchRequest": search_request,
                "staysMapSearchRequestV2": search_request.copy(),
                "isLeanTreatment": False,
                "aiSearchEnabled": False,
                "skipExtendedSearchParams": False
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": GRAPHQL_HASH
                }
            }
        }
    
    all_listings = []
    cursor = None
    page_count = 0
    global_position = 0  # Compteur de position globale
    
    try:
        while page_count < MAX_PAGES:
            page_count += 1
            
            payload = build_graphql_payload(cursor)
            
            response = curl_requests.post(
                f"https://www.airbnb.com/api/v3/StaysSearch/{GRAPHQL_HASH}?operationName=StaysSearch&locale=en&currency={CURRENCY}",
                headers=headers,
                json=payload,
                impersonate="chrome120",
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"      ⚠️ HTTP {response.status_code}", flush=True)
                break
            
            data = response.json()
            
            if "errors" in data:
                print(f"      ⚠️ Erreur GraphQL: {data['errors'][0].get('message', 'Unknown')}", flush=True)
                break
            
            # Extraire les listings
            stays_search = data.get("data", {}).get("presentation", {}).get("staysSearch", {})
            results = stays_search.get("results", {})
            search_results = results.get("searchResults", [])
            
            page_listings = []
            
            for result in search_results:
                global_position += 1  # Incrémenter AVANT de traiter
                
                # Extraire room_id depuis base64
                room_id = None
                dsl = result.get("demandStayListing", {})
                if dsl:
                    encoded_id = dsl.get("id", "")
                    if encoded_id:
                        try:
                            decoded = base64.b64decode(encoded_id).decode("utf-8")
                            if ":" in decoded:
                                room_id = decoded.split(":")[1]
                        except:
                            pass
                
                if not room_id:
                    continue
                
                # Extraire le prix
                price = None
                structured_price = result.get("structuredDisplayPrice", {})
                if structured_price:
                    primary_line = structured_price.get("primaryLine", {})
                    price = primary_line.get("discountedPrice") or primary_line.get("price")
                
                # Extraire rating depuis avgRatingLocalized (ex: "4.98 (42)")
                avg_rating = ""
                reviews_count = ""
                rating_str = result.get("avgRatingLocalized", "")
                if rating_str:
                    match = re.match(r"([\d.]+)\s*\((\d+)\)", rating_str)
                    if match:
                        avg_rating = match.group(1)
                        reviews_count = match.group(2)
                    elif re.match(r"^[\d.]+$", rating_str):
                        avg_rating = rating_str
                
                # Vérifier badges depuis la recherche
                is_guest_favorite_search = False
                badges = result.get("badges", [])
                for badge in badges:
                    logging_ctx = badge.get("loggingContext", {})
                    if logging_ctx.get("badgeType") == "GUEST_FAVORITE":
                        is_guest_favorite_search = True
                        break
                
                page_listings.append({
                    "position": global_position,
                    "page": page_count,
                    "room_id": str(room_id),
                    "title": result.get("title", "") or result.get("subtitle", ""),
                    "price": price,
                    "avg_rating_search": avg_rating,
                    "reviews_count_search": reviews_count,
                    "is_guest_favorite_search": is_guest_favorite_search,
                })
            
            all_listings.extend(page_listings)
            
            print(f"      📄 Page {page_count}: +{len(page_listings)} listings (total: {len(all_listings)}, pos: {global_position})", flush=True)
            
            if not page_listings:
                break
            
            # Pagination
            pagination_info = results.get("paginationInfo", {})
            cursor = pagination_info.get("nextPageCursor")
            
            if not cursor:
                break
            
            time.sleep(DELAY_BETWEEN_PAGES)
        
        return all_listings
        
    except Exception as e:
        print(f"      ❌ Erreur recherche: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return all_listings


# ==============================================================================
# RÉCUPÉRATION DES DÉTAILS
# ==============================================================================

def get_listing_details(room_id):
    """Récupère les détails complets d'un listing via pyairbnb."""
    try:
        details = pyairbnb.get_details(
            room_id=room_id,
            currency=CURRENCY,
            proxy_url="",
            language="en",
        )
        return details
    except Exception as e:
        return None


def extract_details(details):
    """Extrait les informations pertinentes des détails du listing."""
    result = {
        # Variables existantes
        "room_type": "",
        "bedrooms": "",
        "beds": "",
        "bathrooms": "",
        "guests_capacity": "",
        "rating_overall": "",
        "rating_accuracy": "",
        "rating_cleanliness": "",
        "rating_checkin": "",
        "rating_communication": "",
        "rating_location": "",
        "rating_value": "",
        "reviews_count": "",
        "host_id": "",
        "host_name": "",
        "is_superhost": False,
        "is_guest_favorite": False,
        "top_percent": "",
        "badges": "",
        # === NOUVELLES VARIABLES ===
        "instant_book": False,
        "cancellation_policy": "",
        "has_pool": False,
        "has_gym": False,
        "has_parking": False,
        "amenities_count": 0,
        "min_nights": "",
        "max_nights": "",
    }
    
    if not details:
        return result
    
    # Room type
    room_type = details.get("room_type")
    if room_type:
        result["room_type"] = str(room_type)
    
    # Capacité
    person_capacity = details.get("person_capacity")
    if person_capacity:
        result["guests_capacity"] = str(person_capacity)
    
    # Bedrooms, beds, bathrooms depuis sub_description
    sub_desc = details.get("sub_description", {})
    if sub_desc and isinstance(sub_desc, dict):
        items = sub_desc.get("items", [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, str):
                    item_lower = item.lower()
                    match = re.match(r"(\d+)", item)
                    if match:
                        num = match.group(1)
                        if "bedroom" in item_lower:
                            result["bedrooms"] = num
                        elif "bed" in item_lower and "bedroom" not in item_lower:
                            result["beds"] = num
                        elif "bath" in item_lower:
                            result["bathrooms"] = num
    
    # Ratings
    rating_data = details.get("rating")
    if rating_data and isinstance(rating_data, dict):
        if rating_data.get("accuracy") is not None:
            result["rating_accuracy"] = str(rating_data["accuracy"])
        if rating_data.get("cleanliness") is not None:
            result["rating_cleanliness"] = str(rating_data["cleanliness"])
        if rating_data.get("checking") is not None:
            result["rating_checkin"] = str(rating_data["checking"])
        if rating_data.get("communication") is not None:
            result["rating_communication"] = str(rating_data["communication"])
        if rating_data.get("location") is not None:
            result["rating_location"] = str(rating_data["location"])
        if rating_data.get("value") is not None:
            result["rating_value"] = str(rating_data["value"])
        if rating_data.get("guest_satisfaction") is not None:
            result["rating_overall"] = str(rating_data["guest_satisfaction"])
        if rating_data.get("review_count") is not None:
            result["reviews_count"] = str(rating_data["review_count"])
    
    # Host
    host_data = details.get("host")
    if host_data and isinstance(host_data, dict):
        if host_data.get("id"):
            result["host_id"] = str(host_data["id"])
        if host_data.get("name"):
            result["host_name"] = str(host_data["name"])
    
    # Superhost et Guest Favorite
    if details.get("is_super_host"):
        result["is_superhost"] = True
    if details.get("is_guest_favorite"):
        result["is_guest_favorite"] = True
    
    # Top X% depuis highlights
    highlights = details.get("highlights", [])
    badges_list = []
    
    if isinstance(highlights, list):
        for highlight in highlights:
            if isinstance(highlight, dict):
                title = highlight.get("title", "")
                subtitle = highlight.get("subtitle", "")
                combined = f"{title} {subtitle}".lower()
                
                if "top 1%" in combined:
                    result["top_percent"] = "1"
                elif "top 5%" in combined:
                    result["top_percent"] = "5"
                elif "top 10%" in combined:
                    result["top_percent"] = "10"
    
    # Construire badges
    final_badges = []
    if result["is_superhost"]:
        final_badges.append("Superhost")
    if result["is_guest_favorite"]:
        final_badges.append("Guest Favorite")
    if result["top_percent"]:
        final_badges.append(f"Top {result['top_percent']}%")
    
    result["badges"] = " | ".join(final_badges)
    
    # ==========================================================================
    # NOUVELLES VARIABLES
    # ==========================================================================
    
    # Instant Book
    if details.get("is_instant_bookable"):
        result["instant_book"] = True
    elif details.get("instant_book"):
        result["instant_book"] = True
    elif details.get("instant_bookable"):
        result["instant_book"] = True
    
    # Politique d'annulation
    cancel_policy = details.get("cancellation_policy")
    if cancel_policy:
        if isinstance(cancel_policy, dict):
            result["cancellation_policy"] = cancel_policy.get("name", "") or \
                                            cancel_policy.get("policy_name", "") or \
                                            cancel_policy.get("category", "") or \
                                            cancel_policy.get("type", "")
        elif isinstance(cancel_policy, str):
            result["cancellation_policy"] = cancel_policy
    
    # Min/Max nuits
    if details.get("min_nights"):
        result["min_nights"] = str(details["min_nights"])
    elif details.get("minimum_nights"):
        result["min_nights"] = str(details["minimum_nights"])
    
    if details.get("max_nights"):
        result["max_nights"] = str(details["max_nights"])
    elif details.get("maximum_nights"):
        result["max_nights"] = str(details["maximum_nights"])
    
    # Équipements (amenities)
    amenities = details.get("amenities", [])
    if isinstance(amenities, list):
        result["amenities_count"] = len(amenities)
        
        # Convertir en texte pour recherche
        amenities_lower = [str(a).lower() for a in amenities]
        amenities_str = " ".join(amenities_lower)
        
        # Piscine
        if any(word in amenities_str for word in ["pool", "piscine", "swimming"]):
            result["has_pool"] = True
        
        # Salle de sport
        if any(word in amenities_str for word in ["gym", "fitness", "sport", "exercise", "workout"]):
            result["has_gym"] = True
        
        # Parking
        if any(word in amenities_str for word in ["parking", "garage", "stationnement"]):
            result["has_parking"] = True
    
    return result


# ==============================================================================
# EXÉCUTION D'UN SCÉNARIO
# ==============================================================================

def run_scenario(run_id, days_from_now, nights, guests):
    """Exécute un scénario complet et retourne les listings avec leurs détails."""
    
    print(f"\n{'='*60}", flush=True)
    print(f"🔬 SCÉNARIO: {run_id}", flush=True)
    print(f"   📅 Check-in: +{days_from_now} jours | Durée: {nights} nuits", flush=True)
    print(f"   👥 Voyageurs: {guests}", flush=True)
    print(f"{'='*60}", flush=True)
    
    # Calculer les dates
    checkin_date = datetime.now() + timedelta(days=days_from_now)
    checkout_date = checkin_date + timedelta(days=nights)
    check_in = checkin_date.strftime("%Y-%m-%d")
    check_out = checkout_date.strftime("%Y-%m-%d")
    
    print(f"\n   📍 Zone: {QUERY} (placeId: {PLACE_ID[:20]}...)", flush=True)
    print(f"   📅 Dates: {check_in} → {check_out}", flush=True)
    
    # Phase 1: Recherche
    print(f"\n   📊 PHASE 1: RECHERCHE", flush=True)
    print(f"   {'-'*40}", flush=True)
    
    listings = search_listings(check_in, check_out, guests)
    
    if not listings:
        print(f"\n   ❌ Aucun listing trouvé!", flush=True)
        return []
    
    print(f"\n   ✅ {len(listings)} listings trouvés", flush=True)
    
    # Phase 2: Détails
    print(f"\n   📊 PHASE 2: DÉTAILS ({len(listings)} listings)", flush=True)
    print(f"   {'-'*40}", flush=True)
    
    for idx, listing in enumerate(listings, start=1):
        room_id = listing["room_id"]
        
        if idx % 20 == 0 or idx == 1:
            print(f"      [{idx}/{len(listings)}] En cours...", flush=True)
        
        details = get_listing_details(room_id)
        
        if details:
            extracted = extract_details(details)
            listing.update(extracted)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    print(f"   ✅ Détails récupérés", flush=True)
    
    # Ajouter les métadonnées du run
    for listing in listings:
        listing["run_id"] = run_id
        listing["checkin_date"] = check_in
        listing["checkout_date"] = check_out
        listing["nights"] = nights
        listing["days_from_now"] = days_from_now
        listing["guests"] = guests
    
    return listings


# ==============================================================================
# EXPORT CSV
# ==============================================================================

def export_to_csv(all_listings, filename):
    """Exporte tous les listings vers un fichier CSV unique."""
    
    fieldnames = [
        # Métadonnées du run
        "run_id",
        "checkin_date",
        "checkout_date",
        "nights",
        "days_from_now",
        "guests",
        # Position
        "position",
        "page",
        # Identifiants
        "room_id",
        "url",
        "title",
        # Infos logement
        "room_type",
        "bedrooms",
        "beds",
        "bathrooms",
        "guests_capacity",
        # Prix
        "price",
        # Ratings
        "rating_overall",
        "rating_accuracy",
        "rating_cleanliness",
        "rating_checkin",
        "rating_communication",
        "rating_location",
        "rating_value",
        "reviews_count",
        # Host
        "host_id",
        "host_name",
        # Badges
        "is_superhost",
        "is_guest_favorite",
        "top_percent",
        "badges",
        # === NOUVELLES COLONNES ===
        "instant_book",
        "cancellation_policy",
        "has_pool",
        "has_gym",
        "has_parking",
        "amenities_count",
        "min_nights",
        "max_nights",
    ]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for listing in all_listings:
            row = {field: listing.get(field, "") for field in fieldnames}
            row["url"] = f"https://www.airbnb.com/rooms/{listing.get('room_id', '')}"
            writer.writerow(row)
    
    print(f"\n📁 Fichier créé: {filename}", flush=True)


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 80)
    print("🔬 AIRBNB SEO RESEARCH")
    print("=" * 80)
    print(f"\n📍 Zone: {QUERY}")
    print(f"👥 Voyageurs: {GUESTS}")
    print(f"💰 Devise: {CURRENCY}")
    
    # Déterminer quels scénarios exécuter
    scenarios_to_run = []
    
    if RUN_ALL:
        scenarios_to_run = SCENARIOS
        print(f"\n🎯 Mode: TOUS les scénarios ({len(SCENARIOS)})")
    else:
        for run_id, days, nights, env_var in SCENARIOS:
            if os.environ.get(env_var, "true").lower() == "true":
                scenarios_to_run.append((run_id, days, nights, env_var))
        print(f"\n🎯 Mode: Sélection manuelle ({len(scenarios_to_run)} scénarios)")
    
    print(f"\n📋 Scénarios à exécuter:")
    for run_id, days, nights, _ in scenarios_to_run:
        print(f"   • {run_id}: +{days} jours, {nights} nuits")
    
    # Exécuter chaque scénario
    all_listings = []
    
    for idx, (run_id, days, nights, _) in enumerate(scenarios_to_run, start=1):
        print(f"\n\n{'#'*80}")
        print(f"# SCÉNARIO {idx}/{len(scenarios_to_run)}")
        print(f"{'#'*80}")
        
        listings = run_scenario(run_id, days, nights, GUESTS)
        all_listings.extend(listings)
        
        print(f"\n   📊 Total cumulé: {len(all_listings)} listings")
        
        # Pause entre scénarios
        if idx < len(scenarios_to_run):
            print(f"\n   ⏳ Pause de {DELAY_BETWEEN_SCENARIOS}s avant le prochain scénario...")
            time.sleep(DELAY_BETWEEN_SCENARIOS)
    
    # Export final
    print(f"\n\n{'='*80}")
    print("📊 EXPORT FINAL")
    print("=" * 80)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"seo_research_{timestamp}.csv"
    export_to_csv(all_listings, filename)
    
    # Résumé
    print(f"\n{'='*80}")
    print("🎉 TERMINÉ!")
    print("=" * 80)
    
    print(f"\n📊 RÉSUMÉ:")
    print(f"   • Scénarios exécutés: {len(scenarios_to_run)}")
    print(f"   • Total listings: {len(all_listings)}")
    
    # Stats par scénario
    print(f"\n📈 PAR SCÉNARIO:")
    for run_id, _, _, _ in scenarios_to_run:
        count = sum(1 for l in all_listings if l.get("run_id") == run_id)
        print(f"   • {run_id}: {count} listings")
    
    # Stats globales
    if all_listings:
        superhosts = sum(1 for l in all_listings if l.get("is_superhost"))
        guest_favorites = sum(1 for l in all_listings if l.get("is_guest_favorite"))
        instant_books = sum(1 for l in all_listings if l.get("instant_book"))
        has_pools = sum(1 for l in all_listings if l.get("has_pool"))
        has_gyms = sum(1 for l in all_listings if l.get("has_gym"))
        has_parkings = sum(1 for l in all_listings if l.get("has_parking"))
        
        print(f"\n📈 STATISTIQUES GLOBALES:")
        print(f"   • Superhosts: {superhosts} ({100*superhosts/len(all_listings):.1f}%)")
        print(f"   • Guest Favorites: {guest_favorites} ({100*guest_favorites/len(all_listings):.1f}%)")
        print(f"\n📈 NOUVELLES VARIABLES:")
        print(f"   • Instant Book: {instant_books} ({100*instant_books/len(all_listings):.1f}%)")
        print(f"   • Avec piscine: {has_pools} ({100*has_pools/len(all_listings):.1f}%)")
        print(f"   • Avec gym: {has_gyms} ({100*has_gyms/len(all_listings):.1f}%)")
        print(f"   • Avec parking: {has_parkings} ({100*has_parkings/len(all_listings):.1f}%)")
        
        # Stats politiques d'annulation
        policies = {}
        for l in all_listings:
            policy = l.get("cancellation_policy", "")
            if policy:
                policies[policy] = policies.get(policy, 0) + 1
        
        if policies:
            print(f"\n📈 POLITIQUES D'ANNULATION:")
            for policy, count in sorted(policies.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   • {policy}: {count} ({100*count/len(all_listings):.1f}%)")
    
    print(f"\n📁 Fichier: {filename}")
    print("=" * 80)


if __name__ == "__main__":
    main()
