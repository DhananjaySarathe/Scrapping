# https://selenium-python.readthedocs.io/getting-started.html
# Tip : Do not give same name to file as name of packages.. As this will cause issues while importing packages..


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome()
driver.get("http://www.python.org")
assert "Python" in driver.title
elem = driver.find_element(By.NAME, "q")
elem.clear()
elem.send_keys("pycon")
elem.send_keys(Keys.RETURN)

# Wait for page to load after search
time.sleep(2)  # Give page time to load

# Try to find h2 elements, with fallback to other elements
e = driver.find_elements(By.TAG_NAME, "p")
print(f"Found {len(e)} h2 elements")  # Debug output

# If no h2 elements, try h3 or other common heading tags
if len(e) == 0:
    e = driver.find_elements(By.TAG_NAME, "p")
    if len(e) == 0:
        # Try finding any headings
        e = driver.find_elements(By.CSS_SELECTOR, "h1, h2, h3, h4")
        print(f"Found {len(e)} heading elements total")

st = ""
for i in e:
    text = i.text.strip()
    if text:  # Only add non-empty text
        st += f"{text}\n"

# Write to file with UTF-8 encoding
with open("python.org.txt", "w", encoding="utf-8") as f:
    f.write(st)

print(f"Written {len(st)} characters to file")

assert "No results found." not in driver.page_source
a = input()  #This will keep the browser open until you press enter..
# driver.close()
