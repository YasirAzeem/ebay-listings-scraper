from cmath import e
from urllib import request
import scrapy
from bs4 import BeautifulSoup
import re, json
import sys, random
import mysql.connector
import json
import datetime
import urllib.parse
from slugify import slugify
from threading import Thread



class EbaySpider(scrapy.Spider):
    name = 'ebay'
    allowed_domains = ['ebay.com']
    start_urls = ['http://ebay.com/']
    count = 0

    
    def start_requests(self):
        kws = [x.replace(',','') for x in open('kws.txt','r').read().split('\n') if x]
        for kw in kws[5000:]:
            url =  f"https://www.ebay.com/sch/i.html?_nkw={urllib.parse.quote_plus(kw)}&_sacat=0&_ipg=240&rt=nc&LH_PrefLoc=1"    
            yield scrapy.Request(url=url,callback=self.parse,meta={'kw':kw,'count':1})



    def get_num(self,line):
        return re.findall(r"[-+]?(?:\d*\.\d+|\d+)",line)


    def parse(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        listings = soup.find_all('li',{'class':'s-item'})
        more_keywords = soup.find('div',{'class':'srp-related-searches'})
        if more_keywords:
            try:
                more_keywords = [(x.text.strip(),slugify(x.text.strip())) for x in more_keywords.find_all('a')]
                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES ( %s, %s);'''
                self.cursor.executemany(sql_update_query,more_keywords)
                self.conn.commit()
            except Exception as e:
                self.conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
                self.cursor = self.conn.cursor(buffered=True)
                print('More Keywords:1 ',e)
        for inx, li in enumerate(listings[:30]):
            item = {}
            item['keyword'] = response.meta['kw']
            item['base'] = response.request.url
            item['Index'] = int(response.meta['count']) + inx
            try:
                item['title'] = li.find('span',{'role':'heading'}).text.strip()
            except:
                try:
                    item['title'] = li.find('a',{'class':'s-item__link'}).text.strip()
                except:
                    item['title'] = None
                    
                    continue
            if item['title']:
                item['slug'] = slugify(item['title'])
            else:
                item['slug'] = ""
            item['product_id'] = li.find('a',{'class':'s-item__link'}).get('href').split('itm/')[-1].split('?')[0]
            if item['title']=="Shop on eBay":
                continue
            item['url'] =  f"https://www.ebay.com/itm/{item['product_id']}/"
            if "Brand New" in str(li):
                item['Condition'] = "New"
            item['price'] = li.find('span',{'class':'s-item__price'})
            if item['price']:
                item['price'] = self.get_num(item['price'].text.replace(',',''))
                if item['price']:
                    item['price'] = item['price'][0]
                else:
                    item['price'] = None
            reviews = li.find('div',{'class':'s-item__reviews'})
            item['rating'] = None
            item['rating_count'] = 0
            if reviews:
                item['rating'] = reviews.find('span')
                if item['rating']:
                    item['rating'] = self.get_num(item['rating'].text)
                    if item['rating']:
                        item['rating'] = item['rating'][0]
                
                item['rating_count'] = li.find('span',{'class':"s-item__reviews-count"})
                if item['rating_count']:
                    item['rating_count']  = abs(int(self.get_num(item['rating_count'].text.replace(',','').strip())[0]))
            
            shipping_cost = -1
            shipping = li.find('span',{'class':"s-item__shipping s-item__logisticsCost"})
            if shipping:
                shipping_cost = shipping.text
                
                if "free" in shipping_cost.lower():
                    shipping_cost = 0
                elif shipping_cost.lower()=="shipping not specified":
                    shipping_cost = -1
                else:
                    sh = self.get_num(shipping_cost)
                    if sh:
                        shipping_cost = sh[0]

            item['Shipping'] = shipping_cost
            item['seller'] = None
            img = li.find('div',{'class':'s-item__image'})
            if img:
                img = img.find('img')
                if img:
                    img = img.get('src')
            item['Image_URLs'] = img
            
            yield scrapy.Request(url=item['url'], callback=self.parse2, meta={'item':item})
            # yield item

        # pagination = soup.find('a',{'aria-label':'Go to next search page'})
        # if pagination:
        #     url = pagination.get('href')
        #     if url:
        #         if int(response.meta['count'])+len(listings)<300:
        #             yield scrapy.Request(url=url, callback=self.parse,meta={'kw':response.meta['kw'],'count':int(response.meta['count'])+len(listings)})
        #         else:
        #             return

    
    def parse2(self, response):
        s = re.sub('<br\s*?>', '\n', str(response.body))
        soup = BeautifulSoup(s.replace('\\n','\n'),'lxml')
        item = response.meta['item']
        if not item.get('condition'):
            item['Condition'] = soup.find('div',{'class':'d-item-condition-value'})
            if item['Condition']:
                item['Condition'] = item['Condition'].find('span').text
                if "Like New" in item['Condition']:
                    item['Condition'] = "Like New"
                if "New" in item['Condition']:
                    item['Condition'] = "New"
                if "Pre-owned" in item['Condition']:
                    item['Condition'] = "Pre-owned"
                if "Used" in item['Condition']:
                    item['Condition'] = "Used"
                if "not specified" in item['Condition']:
                    item['Condition'] = None
                
            
        item['title'] = soup.find('h1').text.strip()
        isAvailable = False
        if "no longer available" not in str(soup).lower():
            isAvailable = True
        emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticons
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
        "+", flags=re.UNICODE)

        if emoji_pattern.search(item['title']):
            item['title'] = item['title'].encode('unicode-escape')

        
        # item['Image_URLs'] = ",".join([x.find('img').get('src') for x in soup.find_all('li', {'class':'v-pic-item'}) if x.find('img')])
        # if not item['Image_URLs']:
        #     item['Image_URLs'] = soup.find(id='icImg')
        #     if item['Image_URLs']:
        #         item['Image_URLs'] = item['Image_URLs'].get('src')


        # if not item['Image_URLs']:
        #     images = soup.find_all('button',{'class':'image'})
        #     if images:
        #         item['Image_URLs'] = ",".join([x.find('img').get('src').replace('-l64.','-l1600.') for x in images if x.find('img')])

        
        brand = soup.find('span',{'item-prop':'brand'})
        if brand:
            brand = brand.find('span').text.strip()
        item['Brand'] = brand
        material = None
        color = None
        weight = None
        length = None
        height = None
        width = None
        specs = soup.find_all('div',{'class':'ux-labels-values__labels'})
        for sp in specs:
            if "material" in sp.text.lower():
                material = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "color" in sp.text.lower():
                color = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "weight" in sp.text.lower():
                weight = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "height" in sp.text.lower():
                height = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "width" in sp.text.lower():
                width = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "length" in sp.text.lower():
                length = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()

        item['Material'] = material
        more_specs = soup.find('div',{'data-testid':'x-about-this-item'})
        more_specs_dict = {}
        if more_specs:
            
            rows = more_specs.find_all('div',{'class':'ux-labels-values__labels'})
            for row in rows:
                label = row.text.split(':')[0].strip()
                if len(label.split())<3:
                    if "condition" in label.lower():
                        label = label.lower().replace("condition","item_condition")
                    if "Brand" in label:
                        item['Brand'] = row.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
                    more_specs_dict[label] = row.find_next('div',{'class':'ux-labels-values__values'}).text.strip()

        instockqty = soup.find(id='qtySubTxt')
        if instockqty:
            instockqty = instockqty.text.split()[0]
            
        sales = soup.find('div',{'class':'soldwithfeedback'})
        if sales:
            sales = sales.text.split()[0].replace(',','')
        else:
            sales = None
        expirationTime = None
        if soup.find(id='MaxBidId'):
            isAuction = True
            expirationTime = soup.find('span',{'class':'vi-tm-left'})
            if expirationTime:
                expirationTime = expirationTime.text.replace('\\t','').replace('\\r','').replace('\n','').strip()
        else:
            isAuction = False
        description = soup.find(id='descriptioncontent')
        if description:
            description = description.text.strip().strip()
        else:
            description = soup.find('div',{'class':'ux-layout-section__textual-display--description'})
            if description:
                description = description.text.strip().strip()


        item['Long Desc'] = description

        if not item['Long Desc']:
            description = soup.find(id='ds_div')
            if description:
                item['Long Desc'] = description.text.strip()
        item['Short Desc'] = ""
        cats = soup.find('nav',{'aria-labelledby':'listedInCat'})
        if cats:
            cats = cats.find_all('a',{'class':'seo-breadcrumb-text'})
        else:
            cats = soup.find_all('nav')
            for c in cats:
                if "You are here" in str(c):
                    cats = c
                    break
            if type(cats)==list:
                cats = None
            else:
                cats = cats.find_all('a',{'class':'seo-breadcrumb-text'})
        try:
            if cats:
                item['categories'] = ">".join([x.text for x in cats])
                if "Back to search results" in item['categories']:
                    item['categories'] = None
            else:
                item['categories'] = None
        except:
            item['categories'] = None
        
        
        
        item['Color'] = color    
        mbuy_dict =[]
        multibuy = soup.find_all('a',{'class':'vi-vpqp-pills'})
        if multibuy:
            for a in multibuy:
                mb = {}
                amount = self.get_num(a.find('div').text)
                if amount:
                    mb['quantity'] = amount[0]
                    try:
                        mb['price'] = self.get_num(a.find('div',{'class':'vpqp-price'}).text)[0]
                    except:
                        mb['price'] = None
                    mbuy_dict.append(mb)
        if not mbuy_dict:
            multibuy = soup.find_all('span',{'class':'vi-volume'})
            if multibuy:
                for a in multibuy:
                    mb = {}
                    amount = self.get_num(a.text)
                    if amount:
                        mb['quantity'] = amount[0]
                        mb['price'] = self.get_num(a.find_next('span',{'class':'vi-vprice'}).text)[0]
                    mbuy_dict.append(mb)
        item['store_name'] = soup.find('div',{'data-testid':'str-title'})
        if item['store_name']:
            item['store_name'] = item['store_name'].text.strip()
        item['Specifications'] = json.dumps({"Weight":weight,"Length":length, "Width":width, "Height": height})
        item['Site Specific Data'] = json.dumps({'store_name':item['store_name'],'item_specs':more_specs_dict,'quantity_sold':sales, "inStock": isAvailable, "quantity_based_prices": mbuy_dict, "available": instockqty, "auction": {"active":isAuction, "expiration":expirationTime}})
        item['reviews'] = []
        reviews_list = soup.find('div',{'class':'reviews'})
        if reviews_list:
            reviews_list = reviews_list.find_all('div',{'itemprop':' review'})
            for rev in reviews_list:
                review = {}
                author = rev.find('a',{'class':'review-item-author'})
                if author:
                    review['review_by']=author.text.strip()
                review['review_datetime'] = rev.find('span',{'itemprop':'datePublished'}.text)
                review['text'] = rev.find('p',{'itemprop':'reviewBody'}).text
                review['rating'] = rev.find('div',{'class':'ebay-star-rating'}).get('aria-label').split()[0]
                item['reviews'].append(review)
        item['reviews_count'] = len(item['reviews'])
        
        # t = Thread(target=self.upload_entry,args=(item,),daemon=True)
        # t.start()
        # self.upload_entry(item)
        yield item
        
   