# BeautifulSoup

from bs4 import BeautifulSoup

with open("file.html", "r") as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, "html.parser")
# print(soup.prettify())

print(soup.title)
print(soup.title.text)
print(soup.title.string)
print(soup.title.parent)
print(soup.title.parent.name)