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

# Create a BeautifulSoup object for parsing
soup = BeautifulSoup(html_content, 'html.parser')

# ------------------------------
# Extracting Title Information
# ------------------------------

# Print the text inside the <title> tag
print(soup.title.text)  
# Output: The Dormouse's story

# Access the full <title> tag
print("Title tag:", soup.title)  
# Output: <title>The Dormouse's story</title>

# Get the tag name (should return 'title')
print("Title tag name:", soup.title.name)  
# Output: title

# Get only the string (text) within the title tag
print("Title tag string:", soup.title.string)  
# Output: The Dormouse's story

# Get the name of the parent tag (title is inside <head>)
print("Parent tag of title:", soup.title.parent.name)  
# Output: head

# ------------------------------
# Accessing Paragraphs
# ------------------------------

# Access the first paragraph tag <p>
print("First paragraph tag:", soup.p)  
# Output: <p class="title"><b>The Dormouse's story</b></p>

# Access the class attribute of the first paragraph
print("Class attribute of first paragraph:", soup.p['class'])  
# Output: ['title']

# ------------------------------
# Accessing Links (Anchor Tags)
# ------------------------------

# Access the first anchor tag <a>
print("First anchor tag:", soup.a)  
# Output: <a class="sister" href="http://example.com/elsie" id="link1">Elsie</a>

# Find all anchor tags in the document
print("All anchor tags:", soup.find_all('a'))
# Output:
# [
#   <a class="sister" href="http://example.com/elsie" id="link1">Elsie</a>,
#   <a class="sister" href="http://example.com/lacie" id="link2">Lacie</a>,
#   <a class="sister" href="http://example.com/tillie" id="link3">Tillie</a>
# ]

# Find the specific tag with id="link3"
print("Tag with id=link3:", soup.find(id="link3"))
# Output: <a class="sister" href="http://example.com/tillie" id="link3">Tillie</a>
