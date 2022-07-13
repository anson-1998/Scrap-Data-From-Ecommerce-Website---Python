#!/usr/bin/env python
# coding: utf-8

# In[5]:


from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import re
from sqlalchemy import create_engine
import urllib
from Data_URL import URL
import Scrap_Product_Info_Logic as Scrap

# Establish connection to local sql server
def connect_to_sql(connection_string):
    quoted = urllib.parse.quote_plus(connection_string)

    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(quoted))
    conn = engine.connect()
    return conn

# Web scraping workflow
class ReturnProductData:
    def __init__(self, ChromeDriver_PATH, Chrome_option):
        self.ChromeDriver_PATH = ChromeDriver_PATH
        self.Chrome_option = Chrome_option
        self.webdriver = None
        self.ResultData = None
        self.ProdcutWithUntracableItems = []
        self.ProdcutWithUntracableItems_Index = []
        self.index_of_product_being_process = None
        
    def CreateWebdriver(self, URL):
        self.webdriver = webdriver.Chrome(options = self.Chrome_option, executable_path = self.ChromeDriver_PATH)
        self.webdriver.get(URL)
        # Wait for at most 10 seconds for searching the targeted web content 
        self.webdriver.implicitly_wait(10)
    
    # Loop over the catalogue for a product category, get each product name and the URL direct to the product detail page
    def Get_productAndURL(self, root_url, relative_url):
        page_number = 0
        x = Scrap.AddProductInfo()
        while True:
            # Update URL with page number
            Full_URL = f'{root_url}+{relative_url}'
            page_number_index = re.search("page=", Full_URL)
            Full_URL = Full_URL[0:page_number_index.end()] + str(page_number) + Full_URL[page_number_index.end() + 1:]
            # Check for whether the targeted web content is avaliable in that URL
            try:
                self.CreateWebdriver(Full_URL)
                self.webdriver.find_element(By.CLASS_NAME, "product-brief-wrapper")
                # Return the scraped web content
                soup = BeautifulSoup(self.webdriver.page_source, 'html.parser')
                #Excape the loop if no more products to be captured
            except NoSuchElementException:
                break
            finally:
                self.webdriver.quit()
            # Extract the product name and product details page URL for each product   
            allItems = soup.find_all("span", {'class':"product-brief-wrapper"}) 
            for item in allItems:
                try:
                    x.addProductName(soup = item)
                    x.addProductRelative_URL(soup = item, root_url = root_url)
                except AttributeError:
                    print("Problems in extracting Product Name/ URL for directing to product details")
                    raise
            page_number += 1 
        # Insert the captured data into the result dataframe
        self.ResultData = pd.DataFrame(x.ProductData)
        
    # Loop over each of the product detail page, extract the supplementary product information and return dataframe with raw product data    
    def AppendProductInfo(self):
        x = Scrap.AddProductInfo()
        for index, row in self.ResultData.iterrows():
            self.index_of_product_being_process = index
            # Check for whether the targeted content is avaliable in that URL
            try:
                self.CreateWebdriver(row['Product Relative URL'])
                self.webdriver.find_element(By.CLASS_NAME, "productDetailPage")
                # Return the scraped web content
                soup_eachProduct = BeautifulSoup(self.webdriver.page_source, 'html.parser')
            except NoSuchElementException:
                self.ProdcutWithUntracableItems.append(row['Product Name'])
                self.ProdcutWithUntracableItems_Index.append(index)
                continue
            finally:
                self.webdriver.quit()
            # Dynamically add the funtions for capturing each of the product information. Take reference from "Scrap Product info logic.py"
            x.addAverageRating(soup = soup_eachProduct)
            x.addCommentCount(soup = soup_eachProduct)
            x.addPrice(soup = soup_eachProduct)
            x.add_Place_of_origin(soup = soup_eachProduct)

        # If any product detail page cannot be accessed, filter out those products from the result dataframe accordingly
        if len(self.ProdcutWithUntracableItems_Index) > 0:
            self.ResultData.drop(self.ProdcutWithUntracableItems_Index, axis = 0, inplace = True)
        # Append the product information to the result dataframe
        for key in x.ProductData:
            # Return error message if any of the product information is missing for the remaining products
            assert len(x.ProductData[key])== self.ResultData.shape[0] , f"Missing records in {key}"
            self.ResultData.loc[:, key] = x.ProductData[key]
            
# Class contain the data cleansing procedure for each of the stated columns
class CleansingLogic:
    def __init__(self):
        self.columns = {"Average Rating" : self.Edit_AverageRating,
                        "Count Of Comment" : self.Edit_CountOfComment,
                        "Price" : self.Edit_Price}
    def Edit_AverageRating(self, x):
        return float(x)
    
    def Edit_CountOfComment(self, x):
        return int(x)
    
    def Edit_Price(self, x):
        return float(re.search('(\d+\.\d+)', x).group())
    
#To automatically apply the corresponding cleansing procedure for the columns stated in the function argument
def CleanData(df, listOfColumns):
    Cleansed_df = df.copy()
    for cols in listOfColumns:
        Cleansed_df[cols] = Cleansed_df[cols].apply(lambda x : CleansingLogic().columns[cols](x))
    return Cleansed_df

# Return the data with products having the average rating within the stated range, and being sorted by the highest number of comments
def MostCommentsWithRating(df, Rating_Low, Rating_High):
    x = df.loc[(df['Average Rating'] >= float(Rating_Low)) & (df['Average Rating'] <= float(Rating_High))]
    return x.sort_values(by=['Count Of Comment'], ascending=False)


# In[ ]:


#------------------------------------------Input Variables---------------------------------------------------

#The path of the execution program of the Chrome driver program
ChromeDriver_PATH = "C:/Users/honso/OneDrive/Desktop/Chorme driver/Chorme driver version 104/chromedriver.exe"

#The path of the execution program of the Chrome browser
Chrome_option = webdriver.ChromeOptions()
Chrome_option.binary_location = r"C:/Program Files/Google/Chrome Beta/Application/chrome.exe"

#Credentials for connecting to the local sql server
connection_string = (
    "Driver={SQL Server Native Client 11.0};"
    "Server=DESKTOP-A4GVUVN\SQLEXPRESS;"
    "Database={Web Scraping From HKTV Mall};"
    "Trusted_Connection=yes;"
)
# Prodcut category chosen for analyzing. Take reference from "Data_URL.py"
Product_category = "Air Fryer"
# File name of the result dataset
result_filename = 'Air_Fryer_20220705'

#--------------------------------------------Execution----------------------------------------------------

# Generate Raw Products data
New_project = ReturnProductData(ChromeDriver_PATH = ChromeDriver_PATH, Chrome_option = Chrome_option)
New_project.Get_productAndURL(URL().MainURL, URL().RelativeURL_By_Category[Product_category])
New_project.AppendProductInfo()
Raw_data = New_project.ResultData

# Data Cleansing
ColumnsToBeCleaned = ['Average Rating', 'Count Of Comment', 'Price']
Cleaned_data = CleanData(Raw_data, ColumnsToBeCleaned)

# Querying Data with customized range of product average rating, sorted by highest number of comments given
result = MostCommentsWithRating(Cleaned_data, Rating_Low = 4, Rating_High = 5)

# Set up connection to sql server and export the result file there
conn = connect_to_sql(connection_string)
result.to_sql(result_filename,conn, if_exists='replace', index=False)

