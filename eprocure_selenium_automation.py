import datetime as dt
import logging
import os
import time
import traceback
import warnings

import pandas as pd
import pymysql as pms
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests
warnings.filterwarnings(action="ignore")
chromedriver = "/home/astrostockarnav/chromedriver78/chromedriver"
chromedriver = "/home/ayush/geckodriver"
os.environ['webdriver.chrome.driver'] = chromedriver
# display = Display(visible=0, size=(1024, 768))
# display.start()

driver = webdriver.Firefox(executable_path=chromedriver)
time.sleep(2)

TENDER_LINK = []
TENDER_Description = []
Tender_ID = []
Company = []
Estimated_Cost = []
EMD_VALUE = []
LOCATION = []
DEADLINE = []
Publish_Date = []
Tender_Doc_Link = []
list_of_strings = []
with requests.Session() as s:
    for i in range(1, 5):
        url = "https://eprocure.gov.in/cppp/latestactivetendersnew/cpppdata?page={0}".format(i)
        print("Page No: {0}".format(i))
        try:
            driver.get(url=url)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "block-cppp-content")))
        except Exception:
            driver.implicitly_wait(60)
            print(traceback.format_exc())

        time.sleep(5)
        source = driver.page_source
        soup = bs(source, "html.parser")
        ## all table links to individual tenders
        for row in soup.find_all('tr', {"style": "border-bottom: 1px solid #ffffff;background-color: #FAFAFA;"}):
            header = row.find_all('td')
            header_length = 7

            try:
                if len(header) < 1:
                    continue
                if len(header) == header_length:
                    # for num in range(1, 7):
                    element = header[4].find(text=True)
                    TENDER_Description.append(element)
                    link = header[4].find(href=True)
                    TENDER_LINK.append(link["href"])
            except Exception as e:
                print("Error as:", e)
            finally:
                pass
df_tender = pd.DataFrame({"Tender Details": TENDER_Description, "TENDER_LINK": TENDER_LINK})
df_tender = pd.read_csv('test_data.csv')
df_tender.drop_duplicates(inplace=True)
df_tender.to_csv("eprocure_tender.csv")
print(df_tender)

scraping_keywords = ["weld", "welding", "electrode", "wear plate", "saw wire", "saw flux", "tig filler", "mig wire",
                     "gmaw", "gtaw"]
scraping_keywords_2 = ["wear", "plate"]
scraping_keywords_3 = ["saw", "flux"]

output_index = []
for i in range(len(df_tender)):
    words = df_tender.iloc[i]["Tender Details"].split(" ")
    print(words)
    for word in words:
        for item in scraping_keywords:
            if word.lower() == item.lower():
                # print(i)
                output_index.append(i)

    if all(x in words for x in scraping_keywords_2):
        output_index.append(i)

    elif all(x in words for x in scraping_keywords_3):
        output_index.append(i)

df_output = df_tender.loc[output_index]
# print(df_output)
df_output.drop_duplicates(inplace=True)
df_output.index = range(len(df_output))
df_output.to_csv("output.csv")

for row in range(len(df_output)):
    base_url = df_output.iloc[row]["TENDER_LINK"]
    print(base_url)
    try:
        driver.get(base_url)
        WebDriverWait(driver, 20).until(EC.WebDriverException)
    except Exception:
        print(traceback.format_exc())
        driver.implicitly_wait(60)
    html = driver.page_source
    soup_2 = bs(html, "html.parser")
    cmp = soup_2.find_all("div", {"class": "event-dtl word-break line-height"})
    Company.append(cmp[0].text.strip())

    td_tag = soup_2.find_all("td", {"width": "20%"})
    Tender_ID.append(td_tag[0].text.strip())
    EMD_VALUE.append(td_tag[6].text.strip())
    LOCATION.append(td_tag[7].text.strip())
    Publish_Date.append(td_tag[8].text.strip())
    DEADLINE.append(td_tag[13].text.strip())

    tender = soup_2.find_all("div", {"class": "event-dtl word-break line-height"})
    for href in tender:
        if href.text.strip().startswith("http"):
            Tender_Doc_Link.append(href.text.strip())
            '''Find the Tender Estimated Cost'''
            if href.text.strip().endswith(".pdf"):
                Estimated_Cost.append("Estimated cost data is not available")
            elif "dpsdae" in href.text.strip():
                Estimated_Cost.append("Estimated cost data is not available")
            else:
                link = driver.find_element_by_link_text(href.text.strip())
                try:
                    link.click()
                    html_source = driver.page_source
                    price_soup = bs(html_source, "html.parser")
                    tables = price_soup.find_all("td", {"valign": "top"})
                    print(type(tables))
                    print(len(tables))
                    for table in tables:
                        print(type(table))
                        print(len(table))
                        list_of_strings.append(table.text)

                except Exception:
                    print(traceback.format_exc())
                    current_url = driver.current_url
                    driver.implicitly_wait(60)
                    new_url = driver.current_url
                    if new_url == current_url:
                        Estimated_Cost.append("NA")
                    else:
                        print("Page is not being reloaded")
        else:
            pass

print(len(Company), Company)
print(len(Tender_ID), Tender_ID)
print(len(EMD_VALUE), EMD_VALUE)
print(len(LOCATION), LOCATION)
print(len(Publish_Date), Publish_Date)
print(len(DEADLINE), DEADLINE)
print(len(Tender_Doc_Link), Tender_Doc_Link)
print(len(TENDER_LINK), TENDER_LINK)
'''Filter out the tender valuation'''
try:
    for i in range(len(list_of_strings)):
        if list_of_strings[i].strip() == "Tender Value in ₹":
            if str(list_of_strings[i + 1].strip()) != "NA":
                Estimated_Cost.append("INR " + str(list_of_strings[i + 1].strip()))
            else:
                Estimated_Cost.append(str(list_of_strings[i + 1].strip()))
except Exception:
    print(traceback.format_exc())
print(len(Estimated_Cost), Estimated_Cost)

try:
    company_df = pd.DataFrame(
        {"Company_Name": Company, "TENDER_ID": Tender_ID, "EMD_VALUE": EMD_VALUE, "LOCATION": LOCATION, \
         "Publish_Date": Publish_Date, "DEADLINE": DEADLINE, "Tender_Doc_Link": Tender_Doc_Link})
    company_df.drop_duplicates(inplace=True)
    df_output = pd.concat([df_output, company_df], axis=1)
    df_output["Upload_Date"] = dt.datetime.date(dt.datetime.now())
    df_output["Site_Name"] = "Eprocure"
    df_output.drop_duplicates(inplace=True)
    df_output.to_csv("eprocure_emd.csv")
except Exception:
    print(traceback.format_exc())

df_Estimated_Cost = pd.DataFrame({"Estimated_Cost": Estimated_Cost})
df_output = pd.concat([df_output, df_Estimated_Cost], axis=1)
# df_output.drop("TENDER_LINK",axis=1,inplace=True)
df_output.drop_duplicates(inplace=True)
df_output.to_csv("eprocure_final_emd.csv")

'''close the firefox tab for selenium webdriver'''
driver.close()
# display.sendstop()
'''Now insert the final data to the database'''

conn = pms.connect(host="localhost", user="root", password="astro2019#", db="Tender_Scrap", autocommit=True)
cursor = conn.cursor()
df_master = pd.read_sql("SELECT * FROM master_database", conn)
sql_query = "INSERT INTO master_database(Site_Name,Organisation,Tender_ID,\
                                      Publish_Date,Deadline,Tender_Amount,\
                                      EMD_Amount,Location,Tender_Link,\
                                      Tender_Details) VALUES (%s,%s,%s,\
                                      %s,%s,%s,%s,%s,%s,%s)"
if len(df_master) == 0:
    cursor.execute(sql_query,
                   ("empty", "empty", "empty", "empty", "empty", "empty", "empty", "empty", "empty", "empty"))
    conn.commit()
    df_master = pd.read_sql("SELECT * FROM master_database", conn)
else:
    pass
df_database = df_output
# df_database=pd.DataFrame()
# for j in range(len(df_master)):
#   df_subset=df_output[df_output["TENDER_ID"]!=df_master.iloc[j]["Tender_ID"]]
# df_database=pd.concat([df_database,df_subset])
# df_database.to_csv("database.csv")
list_index = []
for i in range(len(df_database)):
    if df_database.iloc[i]["Company_Name"] == "" and df_database.iloc[i]["TENDER_ID"] == "" and df_database.iloc[i][
        "DEADLINE"] == "":
        list_index.append(i)
df_database.drop(list_index, inplace=True)
df_database.fillna("Data Not Available", inplace=True)
df_database.to_csv("database.csv")
sql_query_2 = "INSERT INTO master_database(Site_Name,Organisation,Tender_ID,\
                                      Publish_Date,Deadline,Tender_Amount,\
                                      EMD_Amount,Location,Tender_Link,\
                                      Tender_Details,Upload_Date) VALUES (%s,%s,%s,\
                                      %s,%s,%s,%s,%s,%s,%s,%s)"

try:
    for i in range(len(df_database)):
        cursor.execute(sql_query_2, (df_database.iloc[i]["Site_Name"], df_database.iloc[i]["Company_Name"], \
                                     df_database.iloc[i]["TENDER_ID"], df_database.iloc[i]["Publish_Date"], \
                                     df_database.iloc[i]["DEADLINE"], df_database.iloc[i]["Estimated_Cost"], \
                                     df_database.iloc[i]["EMD_VALUE"], df_database.iloc[i]["LOCATION"], \
                                     df_database.iloc[i]["TENDER_LINK"], df_database.iloc[i]["Tender Details"], \
                                     dt.datetime.date(dt.datetime.now())))
        cursor.execute('DELETE FROM master_database WHERE Site_Name="empty" and TENDER_ID="empty"')
        conn.commit()
except Exception:
    print(traceback.format_exc())

conn.close()
LOG_FILENAME = '/home/astrostockarnav/scripts/arnavscraping/log/eprocure.log'
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.info('Scraping Job Started...')
logging.error("These are the following errors")
logging.debug('debug method started...')
