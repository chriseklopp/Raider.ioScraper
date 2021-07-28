import requests
response = requests.get(
    "https://raider.io/api/v1/mythic-plus/runs?season=season-sl-1&region=world&dungeon=all&page=2")


print(response.json())