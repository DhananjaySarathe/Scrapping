# Outdated and Have dependency issues so not using this..
# Alternative Playwright (modern, fast) , beautifulsoup4 (simple, easy) , selenium (browser automation)

from requests_html import HTMLSession

session = HTMLSession()

r = session.get("https://python.org")

print(r.text)