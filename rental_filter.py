# Ρυθμίσεις
MAX_RENT = 500
TARGET_AREAS = ["ωρωπός", "κάλαμος", "άγιοι απόστολοι"]

# Προσεγγιστικές αποστάσεις από Μαλακάσα (σε km)
DISTANCES_FROM_MALAKASA = {
    "ωρωπός": 10,
    "κάλαμος": 8,
    "άγιοι απόστολοι": 7,
}

# ΕΔΩ βάζεις τις αγγελίες που βρίσκεις (χειροκίνητα από site)
# title: περιγραφή, price: τιμή σε €, area: περιοχή
listings = [
    {"title": "Διαμέρισμα 40τμ στον Ωρωπό, 450€ χωρίς μεσίτη", "price": 450, "area": "ωρωπός"},
    {"title": "Γκαρσονιέρα 30τμ στον Κάλαμο, 520€", "price": 520, "area": "κάλαμος"},
    {"title": "Στούντιο στους Αγίους Αποστόλους, 400€ χωρίς μεσίτη", "price": 400, "area": "άγιοι απόστολοι"},
    # πρόσθεσε κι άλλες εδώ...
]


def filter_listings(listings):
    results = []
    for item in listings:
        area = item["area"].lower()
        price = item["price"]

        if area not in TARGET_AREAS:
            continue

        if price > MAX_RENT:
            continue

        distance = DISTANCES_FROM_MALAKASA.get(area)
        item["distance_km"] = distance
        results.append(item)

    return sorted(results, key=lambda x: x.get("distance_km", 999))


def main():
    filtered = filter_listings(listings)

    print("\nΑΠΟΤΕΛΕΣΜΑΤΑ ΕΩΣ 500€ ΣΕ ΩΡΩΠΟ - ΚΑΛΑΜΟ - ΑΓΙΟΥΣ ΑΠΟΣΤΟΛΟΥΣ\n")
    for item in filtered:
        print(f"- {item['title']}")
        print(f"  Τιμή: {item['price']}€")
        print(f"  Περιοχή: {item['area']}")
        print(f"  Απόσταση από Μαλακάσα: {item.get('distance_km', 'N/A')} km\n")


if __name__ == "__main__":
    main()
