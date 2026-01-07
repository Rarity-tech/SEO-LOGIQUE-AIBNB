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
        "min_nights": "",
        "max_nights": "",
        "amenities_count": 0,
        # === TOUTES LES AMENITIES (80+) ===
        # Essentiels
        "has_wifi": False,
        "has_kitchen": False,
        "has_washer": False,
        "has_dryer": False,
        "has_air_conditioning": False,
        "has_heating": False,
        "has_tv": False,
        "has_hair_dryer": False,
        "has_iron": False,
        "has_hangers": False,
        "has_essentials": False,
        "has_shampoo": False,
        "has_hot_water": False,
        # Parking
        "has_free_parking": False,
        "has_paid_parking": False,
        "has_street_parking": False,
        "has_garage": False,
        "has_ev_charger": False,
        # Extérieur / Loisirs
        "has_pool": False,
        "has_private_pool": False,
        "has_shared_pool": False,
        "has_hot_tub": False,
        "has_gym": False,
        "has_bbq_grill": False,
        "has_outdoor_furniture": False,
        "has_outdoor_dining": False,
        "has_patio_balcony": False,
        "has_garden": False,
        "has_backyard": False,
        "has_fire_pit": False,
        "has_beach_access": False,
        "has_lake_access": False,
        "has_ski_in_out": False,
        # Vues
        "has_city_view": False,
        "has_mountain_view": False,
        "has_ocean_view": False,
        "has_sea_view": False,
        "has_lake_view": False,
        "has_garden_view": False,
        "has_pool_view": False,
        # Cuisine
        "has_coffee_maker": False,
        "has_espresso_machine": False,
        "has_oven": False,
        "has_stove": False,
        "has_microwave": False,
        "has_refrigerator": False,
        "has_freezer": False,
        "has_dishwasher": False,
        "has_dishes_silverware": False,
        "has_cooking_basics": False,
        # Salle de bain
        "has_bathtub": False,
        "has_shower": False,
        "has_bidet": False,
        "has_body_soap": False,
        "has_conditioner": False,
        "has_cleaning_products": False,
        # Chambre / Confort
        "has_bed_linens": False,
        "has_extra_pillows": False,
        "has_blackout_shades": False,
        "has_safe": False,
        "has_fireplace": False,
        "has_ceiling_fan": False,
        # Travail / Bureau
        "has_workspace": False,
        "has_desk": False,
        # Famille / Enfants
        "has_crib": False,
        "has_high_chair": False,
        "has_baby_safety_gates": False,
        "has_children_books_toys": False,
        "has_baby_bath": False,
        # Sécurité
        "has_smoke_alarm": False,
        "has_carbon_monoxide_alarm": False,
        "has_fire_extinguisher": False,
        "has_first_aid_kit": False,
        "has_security_cameras": False,
        "has_lock_on_door": False,
        # Arrivée / Départ
        "has_self_checkin": False,
        "has_lockbox": False,
        "has_smart_lock": False,
        "has_doorman": False,
        # Animaux
        "has_pets_allowed": False,
        # Accessibilité
        "has_step_free_entrance": False,
        "has_wide_entrance": False,
        "has_accessible_parking": False,
        "has_grab_bars": False,
        "has_elevator": False,
        # Divertissement
        "has_netflix": False,
        "has_streaming_service": False,
        "has_cable_tv": False,
        "has_game_console": False,
        "has_books": False,
        "has_board_games": False,
        # Autres
        "has_breakfast": False,
        "has_long_term_stays": False,
        "has_luggage_dropoff": False,
        "has_private_entrance": False,
        "has_sauna": False,
        "has_piano": False,
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
    
    # Instant Book - chemin: host_details.data.presentation.userProfileContainer.userProfile.managedListings[0].instantBookEnabled
    try:
        host_details = details.get("host_details", {})
        if isinstance(host_details, dict):
            data = host_details.get("data", {})
            presentation = data.get("presentation", {})
            user_profile_container = presentation.get("userProfileContainer", {})
            user_profile = user_profile_container.get("userProfile", {})
            managed_listings = user_profile.get("managedListings", [])
            if managed_listings and len(managed_listings) > 0:
                if managed_listings[0].get("instantBookEnabled"):
                    result["instant_book"] = True
    except:
        pass
    
    # Fallback pour instant book
    if not result["instant_book"]:
        if details.get("is_instant_bookable") or details.get("instant_book") or details.get("instant_bookable"):
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
    
    # Min/Max nuits - chemin: calendar[0].days[0].minNights / maxNights
    try:
        calendar = details.get("calendar", [])
        if calendar and len(calendar) > 0:
            days = calendar[0].get("days", [])
            if days and len(days) > 0:
                min_n = days[0].get("minNights")
                max_n = days[0].get("maxNights")
                if min_n is not None:
                    result["min_nights"] = str(min_n)
                if max_n is not None:
                    result["max_nights"] = str(max_n)
    except:
        pass
    
    # Fallback pour min/max nuits
    if not result["min_nights"]:
        if details.get("min_nights"):
            result["min_nights"] = str(details["min_nights"])
        elif details.get("minimum_nights"):
            result["min_nights"] = str(details["minimum_nights"])
    
    if not result["max_nights"]:
        if details.get("max_nights"):
            result["max_nights"] = str(details["max_nights"])
        elif details.get("maximum_nights"):
            result["max_nights"] = str(details["maximum_nights"])
    
    # Équipements (amenities) - DETECTION COMPLETE
    amenities = details.get("amenities", [])
    all_amenity_titles = []
    
    # Extraire tous les titres d'amenities (structure: [{title, values: [{title, available}]}])
    if isinstance(amenities, list):
        for category in amenities:
            if isinstance(category, dict):
                values = category.get("values", [])
                if isinstance(values, list):
                    for item in values:
                        if isinstance(item, dict):
                            title = item.get("title", "")
                            available = item.get("available", True)
                            if title and available:
                                all_amenity_titles.append(title.lower())
    
    result["amenities_count"] = len(all_amenity_titles)
    amenities_str = " ".join(all_amenity_titles)
    
    # === DETECTION DE CHAQUE AMENITY ===
    
    # Essentiels
    result["has_wifi"] = any(x in amenities_str for x in ["wifi", "wi-fi", "internet", "wireless"])
    result["has_kitchen"] = "kitchen" in amenities_str
    result["has_washer"] = any(x in amenities_str for x in ["washer", "washing machine", "laveuse", "lave-linge"])
    result["has_dryer"] = any(x in amenities_str for x in ["dryer", "sèche-linge", "séchoir"])
    result["has_air_conditioning"] = any(x in amenities_str for x in ["air conditioning", "air conditioner", "ac", "climatisation", "a/c"])
    result["has_heating"] = any(x in amenities_str for x in ["heating", "heater", "chauffage", "central heating"])
    result["has_tv"] = any(x in amenities_str for x in [" tv", "television", "télévision", "téléviseur", "hdtv"])
    result["has_hair_dryer"] = any(x in amenities_str for x in ["hair dryer", "hairdryer", "sèche-cheveux"])
    result["has_iron"] = any(x in amenities_str for x in ["iron", "fer à repasser"])
    result["has_hangers"] = "hanger" in amenities_str
    result["has_essentials"] = "essential" in amenities_str
    result["has_shampoo"] = "shampoo" in amenities_str
    result["has_hot_water"] = "hot water" in amenities_str
    
    # Parking
    result["has_free_parking"] = any(x in amenities_str for x in ["free parking", "parking gratuit", "free on-site parking", "free garage"])
    result["has_paid_parking"] = any(x in amenities_str for x in ["paid parking", "parking payant", "parking fee"])
    result["has_street_parking"] = "street parking" in amenities_str
    result["has_garage"] = "garage" in amenities_str
    result["has_ev_charger"] = any(x in amenities_str for x in ["ev charger", "electric vehicle", "charging station", "borne de recharge"])
    
    # Extérieur / Loisirs
    result["has_pool"] = "pool" in amenities_str
    result["has_private_pool"] = any(x in amenities_str for x in ["private pool", "piscine privée"])
    result["has_shared_pool"] = any(x in amenities_str for x in ["shared pool", "piscine partagée", "communal pool"])
    result["has_hot_tub"] = any(x in amenities_str for x in ["hot tub", "jacuzzi", "spa", "whirlpool", "bain à remous"])
    result["has_gym"] = any(x in amenities_str for x in ["gym", "fitness", "exercise", "workout", "sport", "fitness center", "salle de sport"])
    result["has_bbq_grill"] = any(x in amenities_str for x in ["bbq", "grill", "barbecue", "barbeque"])
    result["has_outdoor_furniture"] = any(x in amenities_str for x in ["outdoor furniture", "patio furniture", "garden furniture"])
    result["has_outdoor_dining"] = any(x in amenities_str for x in ["outdoor dining", "al fresco", "dîner extérieur"])
    result["has_patio_balcony"] = any(x in amenities_str for x in ["patio", "balcony", "balcon", "terrace", "terrasse", "deck"])
    result["has_garden"] = any(x in amenities_str for x in ["garden", "jardin", "yard"])
    result["has_backyard"] = any(x in amenities_str for x in ["backyard", "back yard", "arrière-cour"])
    result["has_fire_pit"] = any(x in amenities_str for x in ["fire pit", "firepit", "foyer extérieur"])
    result["has_beach_access"] = any(x in amenities_str for x in ["beach access", "beachfront", "plage"])
    result["has_lake_access"] = any(x in amenities_str for x in ["lake access", "lakefront", "waterfront"])
    result["has_ski_in_out"] = any(x in amenities_str for x in ["ski-in", "ski-out", "ski in", "ski out"])
    
    # Vues
    result["has_city_view"] = any(x in amenities_str for x in ["city view", "city skyline", "vue sur la ville", "urban view"])
    result["has_mountain_view"] = any(x in amenities_str for x in ["mountain view", "vue montagne"])
    result["has_ocean_view"] = any(x in amenities_str for x in ["ocean view", "vue océan", "sea view"])
    result["has_sea_view"] = any(x in amenities_str for x in ["sea view", "vue mer"])
    result["has_lake_view"] = any(x in amenities_str for x in ["lake view", "vue lac"])
    result["has_garden_view"] = any(x in amenities_str for x in ["garden view", "vue jardin"])
    result["has_pool_view"] = any(x in amenities_str for x in ["pool view", "vue piscine"])
    
    # Cuisine
    result["has_coffee_maker"] = any(x in amenities_str for x in ["coffee maker", "coffee machine", "cafetière", "nespresso", "keurig"])
    result["has_espresso_machine"] = any(x in amenities_str for x in ["espresso", "expresso"])
    result["has_oven"] = any(x in amenities_str for x in [" oven", "four"])
    result["has_stove"] = any(x in amenities_str for x in ["stove", "cooktop", "plaque", "cuisinière", "hob"])
    result["has_microwave"] = any(x in amenities_str for x in ["microwave", "micro-onde"])
    result["has_refrigerator"] = any(x in amenities_str for x in ["refrigerator", "fridge", "réfrigérateur", "frigo"])
    result["has_freezer"] = "freezer" in amenities_str or "congélateur" in amenities_str
    result["has_dishwasher"] = any(x in amenities_str for x in ["dishwasher", "lave-vaisselle"])
    result["has_dishes_silverware"] = any(x in amenities_str for x in ["dishes", "silverware", "utensils", "cutlery", "vaisselle", "couverts"])
    result["has_cooking_basics"] = any(x in amenities_str for x in ["cooking basics", "pots", "pans", "oil", "salt"])
    
    # Salle de bain
    result["has_bathtub"] = any(x in amenities_str for x in ["bathtub", "bath tub", "tub", "baignoire"])
    result["has_shower"] = "shower" in amenities_str or "douche" in amenities_str
    result["has_bidet"] = "bidet" in amenities_str
    result["has_body_soap"] = any(x in amenities_str for x in ["body soap", "soap", "savon"])
    result["has_conditioner"] = "conditioner" in amenities_str
    result["has_cleaning_products"] = any(x in amenities_str for x in ["cleaning products", "cleaning supplies"])
    
    # Chambre / Confort
    result["has_bed_linens"] = any(x in amenities_str for x in ["bed linen", "linens", "sheets", "draps"])
    result["has_extra_pillows"] = any(x in amenities_str for x in ["extra pillows", "pillow", "oreiller"])
    result["has_blackout_shades"] = any(x in amenities_str for x in ["blackout", "room-darkening", "rideaux occultants"])
    result["has_safe"] = any(x in amenities_str for x in [" safe", "coffre-fort", "security box"]) and "safety" not in amenities_str
    result["has_fireplace"] = any(x in amenities_str for x in ["fireplace", "cheminée", "indoor fireplace"])
    result["has_ceiling_fan"] = any(x in amenities_str for x in ["ceiling fan", "ventilateur"])
    
    # Travail / Bureau
    result["has_workspace"] = any(x in amenities_str for x in ["workspace", "work space", "dedicated workspace", "espace de travail", "laptop-friendly"])
    result["has_desk"] = any(x in amenities_str for x in ["desk", "bureau", "office"])
    
    # Famille / Enfants
    result["has_crib"] = any(x in amenities_str for x in ["crib", "cot", "berceau", "lit bébé", "pack 'n play"])
    result["has_high_chair"] = any(x in amenities_str for x in ["high chair", "highchair", "chaise haute"])
    result["has_baby_safety_gates"] = any(x in amenities_str for x in ["baby gate", "safety gate", "barrière"])
    result["has_children_books_toys"] = any(x in amenities_str for x in ["children's books", "toys", "jouets", "kids"])
    result["has_baby_bath"] = any(x in amenities_str for x in ["baby bath", "bain bébé"])
    
    # Sécurité
    result["has_smoke_alarm"] = any(x in amenities_str for x in ["smoke alarm", "smoke detector", "détecteur de fumée"])
    result["has_carbon_monoxide_alarm"] = any(x in amenities_str for x in ["carbon monoxide", "co alarm", "co detector", "monoxyde de carbone"])
    result["has_fire_extinguisher"] = any(x in amenities_str for x in ["fire extinguisher", "extincteur"])
    result["has_first_aid_kit"] = any(x in amenities_str for x in ["first aid", "premiers secours", "first-aid"])
    result["has_security_cameras"] = any(x in amenities_str for x in ["security camera", "camera", "surveillance", "caméra"])
    result["has_lock_on_door"] = any(x in amenities_str for x in ["lock on", "door lock", "verrou", "bedroom lock"])
    
    # Arrivée / Départ
    result["has_self_checkin"] = any(x in amenities_str for x in ["self check-in", "self-check-in", "self checkin", "arrivée autonome"])
    result["has_lockbox"] = any(x in amenities_str for x in ["lockbox", "lock box", "key box", "boîte à clés"])
    result["has_smart_lock"] = any(x in amenities_str for x in ["smart lock", "keypad", "code", "serrure connectée", "digital lock"])
    result["has_doorman"] = any(x in amenities_str for x in ["doorman", "concierge", "portier", "building staff"])
    
    # Animaux
    result["has_pets_allowed"] = any(x in amenities_str for x in ["pets allowed", "pet-friendly", "pet friendly", "animaux acceptés", "dog", "cat"])
    
    # Accessibilité
    result["has_step_free_entrance"] = any(x in amenities_str for x in ["step-free", "step free", "no stairs", "sans escalier", "wheelchair"])
    result["has_wide_entrance"] = any(x in amenities_str for x in ["wide entrance", "wide doorway", "entrée large"])
    result["has_accessible_parking"] = any(x in amenities_str for x in ["accessible parking", "handicap parking", "disabled parking"])
    result["has_grab_bars"] = any(x in amenities_str for x in ["grab bar", "grab bars", "barres d'appui"])
    result["has_elevator"] = any(x in amenities_str for x in ["elevator", "lift", "ascenseur"])
    
    # Divertissement
    result["has_netflix"] = "netflix" in amenities_str
    result["has_streaming_service"] = any(x in amenities_str for x in ["streaming", "netflix", "amazon prime", "disney+", "hulu", "hbo", "apple tv"])
    result["has_cable_tv"] = any(x in amenities_str for x in ["cable", "câble", "satellite"])
    result["has_game_console"] = any(x in amenities_str for x in ["game console", "playstation", "xbox", "nintendo", "ps4", "ps5", "gaming"])
    result["has_books"] = any(x in amenities_str for x in ["books", "library", "livres", "bibliothèque", "reading"])
    result["has_board_games"] = any(x in amenities_str for x in ["board game", "games", "jeux de société", "puzzles"])
    
    # Autres
    result["has_breakfast"] = any(x in amenities_str for x in ["breakfast", "petit-déjeuner", "petit déjeuner"])
    result["has_long_term_stays"] = any(x in amenities_str for x in ["long term", "monthly", "long-term", "28+ days"])
    result["has_luggage_dropoff"] = any(x in amenities_str for x in ["luggage dropoff", "luggage storage", "baggage"])
    result["has_private_entrance"] = any(x in amenities_str for x in ["private entrance", "entrée privée", "separate entrance"])
    result["has_sauna"] = "sauna" in amenities_str
    result["has_piano"] = "piano" in amenities_str
    
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
        "min_nights",
        "max_nights",
        "amenities_count",
        # === TOUTES LES AMENITIES ===
        # Essentiels
        "has_wifi",
        "has_kitchen",
        "has_washer",
        "has_dryer",
        "has_air_conditioning",
        "has_heating",
        "has_tv",
        "has_hair_dryer",
        "has_iron",
        "has_hangers",
        "has_essentials",
        "has_shampoo",
        "has_hot_water",
        # Parking
        "has_free_parking",
        "has_paid_parking",
        "has_street_parking",
        "has_garage",
        "has_ev_charger",
        # Extérieur / Loisirs
        "has_pool",
        "has_private_pool",
        "has_shared_pool",
        "has_hot_tub",
        "has_gym",
        "has_bbq_grill",
        "has_outdoor_furniture",
        "has_outdoor_dining",
        "has_patio_balcony",
        "has_garden",
        "has_backyard",
        "has_fire_pit",
        "has_beach_access",
        "has_lake_access",
        "has_ski_in_out",
        # Vues
        "has_city_view",
        "has_mountain_view",
        "has_ocean_view",
        "has_sea_view",
        "has_lake_view",
        "has_garden_view",
        "has_pool_view",
        # Cuisine
        "has_coffee_maker",
        "has_espresso_machine",
        "has_oven",
        "has_stove",
        "has_microwave",
        "has_refrigerator",
        "has_freezer",
        "has_dishwasher",
        "has_dishes_silverware",
        "has_cooking_basics",
        # Salle de bain
        "has_bathtub",
        "has_shower",
        "has_bidet",
        "has_body_soap",
        "has_conditioner",
        "has_cleaning_products",
        # Chambre / Confort
        "has_bed_linens",
        "has_extra_pillows",
        "has_blackout_shades",
        "has_safe",
        "has_fireplace",
        "has_ceiling_fan",
        # Travail / Bureau
        "has_workspace",
        "has_desk",
        # Famille / Enfants
        "has_crib",
        "has_high_chair",
        "has_baby_safety_gates",
        "has_children_books_toys",
        "has_baby_bath",
        # Sécurité
        "has_smoke_alarm",
        "has_carbon_monoxide_alarm",
        "has_fire_extinguisher",
        "has_first_aid_kit",
        "has_security_cameras",
        "has_lock_on_door",
        # Arrivée / Départ
        "has_self_checkin",
        "has_lockbox",
        "has_smart_lock",
        "has_doorman",
        # Animaux
        "has_pets_allowed",
        # Accessibilité
        "has_step_free_entrance",
        "has_wide_entrance",
        "has_accessible_parking",
        "has_grab_bars",
        "has_elevator",
        # Divertissement
        "has_netflix",
        "has_streaming_service",
        "has_cable_tv",
        "has_game_console",
        "has_books",
        "has_board_games",
        # Autres
        "has_breakfast",
        "has_long_term_stays",
        "has_luggage_dropoff",
        "has_private_entrance",
        "has_sauna",
        "has_piano",
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
        
        print(f"\n📈 STATISTIQUES GLOBALES:")
        print(f"   • Superhosts: {superhosts} ({100*superhosts/len(all_listings):.1f}%)")
        print(f"   • Guest Favorites: {guest_favorites} ({100*guest_favorites/len(all_listings):.1f}%)")
        print(f"   • Instant Book: {instant_books} ({100*instant_books/len(all_listings):.1f}%)")
        
        # Top amenities
        amenity_stats = {
            "WiFi": sum(1 for l in all_listings if l.get("has_wifi")),
            "Kitchen": sum(1 for l in all_listings if l.get("has_kitchen")),
            "Pool": sum(1 for l in all_listings if l.get("has_pool")),
            "Gym": sum(1 for l in all_listings if l.get("has_gym")),
            "AC": sum(1 for l in all_listings if l.get("has_air_conditioning")),
            "Parking": sum(1 for l in all_listings if l.get("has_free_parking")),
            "Washer": sum(1 for l in all_listings if l.get("has_washer")),
            "TV": sum(1 for l in all_listings if l.get("has_tv")),
            "Workspace": sum(1 for l in all_listings if l.get("has_workspace")),
            "Elevator": sum(1 for l in all_listings if l.get("has_elevator")),
        }
        
        print(f"\n📈 TOP AMENITIES:")
        for name, count in sorted(amenity_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {name}: {count} ({100*count/len(all_listings):.1f}%)")
        
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
