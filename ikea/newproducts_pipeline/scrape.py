import requests
import json

# IKEA API endpoint
url = "https://sik.search.blue.cdtapps.com/my/en/search?c=listaf&v=20241114"

# Headers (keeping them as close to the browser request as possible)
headers = {
    "authority": "sik.search.blue.cdtapps.com",
    "method": "POST",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,ms;q=0.8,zh-CN;q=0.7,zh;q=0.6",
    "content-type": "application/json",  # Ensure it's JSON
    "origin": "https://www.ikea.com",
    "priority": "u=1, i",
    "referer": "https://www.ikea.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "session-id": "4f52d094-9436-45e8-9d5b-495ebc57f005",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

# JSON payload (mimicking the IKEA request structure)
payload = {
    "searchParameters": {
        "input": "new_product",  # Change this to your search term
        "type": "SPECIAL"
    },
    "optimizely": {
        "listing_3086_ratings_and_review_popup": None,
        "listing_3044_increase_mobile_product_image": None,
        "sik_null_test_20250205_default": "a"
    },
    "isUserLoggedIn": False,
    "components": [
        {
            "component": "PRIMARY_AREA",
            "columns": 4,
            "types": {
                "main": "PRODUCT",
                "breakouts": ["PLANNER", "LOGIN_REMINDER", "MATTRESS_WARRANTY"]
            },
            "filterConfig": {
                "subcategories-style": "tree-navigation",
                "max-num-filters": 5
            },
            "sort": "RELEVANCE",
            "window": {
                "offset": 0,  # Change this for pagination (0 for first page, 24 for second page, etc.)
                "size": 24  # Number of products per request
            }
        }
    ]
}

# Send the request
response = requests.post(url, headers=headers, json=payload)

# Print response details
print("Status Code:", response.status_code)  # 200 if successful
try:
    print(json.dumps(response.json(), indent=2))  # Pretty print JSON response
except json.JSONDecodeError:
    print(response.text)  # Print raw text if not JSON
