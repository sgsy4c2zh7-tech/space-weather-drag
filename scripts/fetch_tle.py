import requests

url = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle"
res = requests.get(url)

with open("docs/data/latest.tle", "w") as f:
    f.write(res.text)
