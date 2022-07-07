#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import re


# In[2]:

class AddProductInfo:
    def __init__(self):
        self.ProductData = {}
        
    def AddDataToList(self, ListName, data):
        try:
            self.ProductData[ListName].append(data)
        except KeyError:
            self.ProductData[ListName] = [data]
    
    def addProductName(self, soup):
        data = soup.find("div", {'class':"brand-product-name"}).get_text()
        self.AddDataToList("Product Name", data)
        
    def addProductRelative_URL(self, soup, root_url):
        data = soup.find("a", rel=True, class_=False)['href']
        data = f'{root_url}+{data}'
        self.AddDataToList("Product Relative URL", data)
               
    def addAverageRating(self, soup):
        data = soup.find("span", {'class': "averageRating"}).get_text()
        self.AddDataToList("Average Rating", data)
        
    def addCommentCount(self, soup):
        data = soup.find("span", {'class': "comment__count"}).get_text()
        self.AddDataToList("Count Of Comment", data)
        
    def addPrice(self, soup):
        data = soup.find("div", {'class': "price"}).get_text()
        self.AddDataToList("Price", data)
        
    def add_Place_of_origin(self, soup):
        if soup.find_all("tr", {'class': "productPackingSpec"}) == []:
            data = 'N/A'
        else:
            for item in soup.find_all("tr", {'class': "productPackingSpec"}):
                if item.find("span"):
                    data = item.find_all("span")[1].text
                else:
                    data = 'N/A'             
        self.AddDataToList("Place Of Origin", data)


