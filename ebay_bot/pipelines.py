# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from slugify import slugify
import mysql.connector
import datetime

class MySQLDataPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
        self.cursor = self.conn.cursor(buffered=True)
        self.conn.autocommit = False



    def process_item(self, item, spider):
        for i in list(item.keys()):
            if not item.get(i):
                item[i]=None
            if type(item[i])==list or type(item[i])==dict:
                item[i] = json.dumps(item[i])
            elif type(item[i])==tuple:
                pass
        name =  "ebay"
        try:
            item_slug = slugify(item['Brand'])
        except:
            item_slug = ""
        sql_update_query = f'''SELECT id from sql_usedpick_com.brand_list where name = "{item['Brand']}"'''
        self.cursor.execute(sql_update_query)
        x = self.cursor.fetchone()
        if x:
            brand_id, = x
            if type(brand_id)==tuple:
                brand_id = list(brand_id)[0]
        else:
            sql_update_query = f'''INSERT INTO sql_usedpick_com.brand_list (name, slug) VALUES ("{item['Brand']}","{item_slug}");'''
            self.cursor.execute(sql_update_query)
            brand_id = self.cursor.lastrowid
        cat = item['categories']
        if cat:
            all_cats_list = [x.strip() for x in cat.split('>') if x]
            indx_dict = {}
            for kn, cat in enumerate(all_cats_list):
                indx_dict[cat] = kn
            cats = ['"'+x.strip()+'"' for x in all_cats_list if x]
            sql_update_query = f'''SELECT * from sql_usedpick_com.categories where category_name IN ({",".join(cats)}) AND type = "{name}";'''
            self.cursor.execute(sql_update_query)
            x = self.cursor.fetchall()
            
            cats_dict = {}
            for c in x:
                c = list(c)
                cats_dict[c[1]] = c[0]
            catid_list = [str(x) for x in list(cats_dict.values())]
            if len(cat.split('>'))==len(list(cats_dict.keys())):
                catid_list = ",".join(catid_list)
            else:
                cats_to_add = [c for c in all_cats_list if c not in list(cats_dict.keys())]
                for i,clr in enumerate(cats_to_add):
                    ct_slug = slugify(clr)
                    parent_id = None
                    for k,ac in enumerate(all_cats_list):
                        if clr==ac:
                            if k==0:
                                break
                            parent_id = cats_dict.get(all_cats_list[k-1])
                            if not parent_id:
                                parent_id = catid_list[-1]
                            break
                    if not parent_id:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}","{ct_slug}","{name}", {indx_dict[clr]}, null, 0, 1);'''
                    else:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}", "{ct_slug}","{name}", {indx_dict[clr]},{parent_id}, 0, 1);'''
                    
                    self.cursor.execute(sql_update_query)
                    catid_list.append(self.cursor.lastrowid)
                    cats_dict[clr] = self.cursor.lastrowid
                catid_list = ",".join([str(x) for x in catid_list])
        else:
            catid_list = None

        sql_update_query = f'''SELECT id from sql_usedpick_com.ebay_products where site_product_id = "{item['product_id']}";'''
        self.cursor.execute(sql_update_query)
        x = self.cursor.fetchone()
       
        if x:
            product_id, = x
        else:
            sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_products (product_reff_url, slug, title, price, site_product_id, images_url, shipping, brand_id, site_specification_data, specification, short_description, long_description, rating, review_count, reviews, item_condition) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
            self.cursor.execute(sql_update_query,(item['url'].split('?')[0], item['slug'], item['title'],item['price'],int(item['product_id']), item['Image_URLs'], item['Shipping'], str(brand_id), item['Site Specific Data'], item['Specifications'], item['Short Desc'], item['Long Desc'], item['rating'], item['rating_count'], item['reviews'], item['Condition']))
            product_id = self.cursor.lastrowid
        
        kw_slug = slugify(item['keyword'])
        sql_update_query = f'''SELECT id from sql_usedpick_com.all_keywords where keyword = "{item['keyword']}";'''
        
        self.cursor.execute(sql_update_query)
        x = self.cursor.fetchone()
        if x:
            kw_id, = x
            
            sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET updated_from_ebay = %s WHERE id = "%s";'''
            self.cursor.execute(sql_update_query,(datetime.datetime.now(),kw_id))
            sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET slug = %s WHERE id = "%s";'''
            self.cursor.execute(sql_update_query,(kw_slug,kw_id))
            
        else:
            sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug, updated_from_ebay) VALUES ( %s, %s, %s);'''
            self.cursor.execute(sql_update_query,(item['keyword'],kw_slug,datetime.datetime.now()))
            kw_id = self.cursor.lastrowid
        
        
        
        sql_update_query = f'''SELECT id from sql_usedpick_com.ebay_keyword where keyword_id = "{kw_id}" AND product_id = {product_id};'''
        
        self.cursor.execute(sql_update_query)
        x = self.cursor.fetchone()
        if x:
            pass
        else:
            sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_keyword (keyword_id, product_id, index_no) VALUES ("{kw_id}", {product_id}, {item["Index"]});'''
            self.cursor.execute(sql_update_query)

        all_category_ids =      [int(x.strip()) for x in catid_list.split(',') if x]

        
        # Categories Table Update

        sql_update_query = f'''SELECT id from sql_usedpick_com.ebay_product_categories where product_id = {product_id} AND keyword_id = {kw_id};'''
        self.cursor.execute(sql_update_query)
        x = self.cursor.fetchone()
        if x:
            pass
        else:
            data = []
            for cid in all_category_ids:
                data.append((kw_id, product_id, cid),)
            sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_product_categories (keyword_id, product_id, category_id) VALUES (%s, %s, %s);'''
            self.cursor.executemany(sql_update_query,data)

        filters = json.loads(item['Site Specific Data'])
        if filters.get('item_specs'):
            specs = filters.get('item_specs')
            for spec in list(specs.keys()):
                try:
                    try:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.attribute_list (name, slug) VALUES (%s, %s);'''
                        self.cursor.execute(sql_update_query,(spec, slugify(spec)))
                        filter_id = self.cursor.lastrowid
                    except Exception as e:
                        # print('Spec Loop ',e)
                        sql_update_query = f'''SELECT id FROM sql_usedpick_com.attribute_list WHERE name = "{spec}";'''
                        self.cursor.execute(sql_update_query)
                        filter_id, = self.cursor.fetchone()
                    value = specs[spec]
                    if spec == "item_condition":
                        continue
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                    self.cursor.execute(sql_update_query,(product_id, kw_id, filter_id, value))
                except:
                    pass


        if item.get('Color'):
            if type(item['Color'])!=list:
                colorss = item['Color'].split(',')
            else:
                colorss = item['Color']
            
            colors = ['"'+x+'"' for x in colorss]
        else:
            colorss = []

        if colorss:
            filter_id = 1
            for color in colorss:
                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                self.cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))


        if item.get('Material'):
            if type(item['Material'])!=list:
                colorss = item['Material'].split(',')
            else:
                colorss = item['Material']
            
            colors = ['"'+x+'"' for x in colorss]
        else:
            colorss = []

        if colorss:
            filter_id = 2
            for color in colorss:
                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.ebay_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                self.cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))
        
        self.conn.commit()
        return item

    
    def close_spider(self, spider):

        ## Close self.cursor & connection to database 
        self.cursor.close()
        self.conn.close()