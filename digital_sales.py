import pandas as pd
import numpy as np
import warnings
import ast
import matplotlib.pyplot as plt

class Data_Ingestion():

    def create_transaction_master_from_sales(self,raw_sales_df):
        def iswholesale(row):
            if str(row['Lineitem name']).lower().find('wholesale') > -1: return 1
            return 0
        raw_sales_df['WHOLESALE'] = raw_sales_df.apply(iswholesale,axis=1)
        def isfree(row):
            if (float(row['Lineitem price'])<0.01)&(float(row['Lineitem price'])>=0.0): return 1
            return 0
        raw_sales_df['FREE'] = raw_sales_df.apply(isfree,axis=1)

        #get stats that are only written at top of transaction
        def finddeadrows(row):
            try:
                x = row['Subtotal']
                if float(x) > 0:
                    return 1
            except ValueError: return 0
            return 0
        raw_sales_df['toprow'] = raw_sales_df.apply(finddeadrows,axis=1)
        toponly = raw_sales_df[raw_sales_df['toprow']==1]
        toponly['Discount Code']=toponly['Discount Code'].fillna("NONE")
        toponly['Shipping Method'] = toponly['Shipping Method'].fillna("UNKNOWN")
        toponly['Discount Amount'] = toponly['Discount Amount'].fillna(0.0)
        toponly=toponly[['Name','Financial Status','Paid at','Fulfillment Status','Currency','Discount Code','Source','Discount Amount','Taxes','Shipping Method','Shipping Street']]
        toponly.columns = ['TRANSACTION_ID','FINANCIAL_STATUS','PAID_TIMESTAMP','FULFILLMENT','CURRENCY','DISCOUNT_CODE','SOURCE','TRANS_DISCOUNT_AMOUNT','TRANS_TAXES','SHIPPING_METHOD','SHIPPING_STREET']

        final_sales = raw_sales_df[['Name', 'Email', 'Created at', 'Lineitem quantity', 'Lineitem price', 'Lineitem sku','WHOLESALE','FREE']]
        final_sales.columns = ['TRANSACTION_ID', 'EMAIL', 'CREATED_TIMESTAMP', 'SKU_QTY', 'SKU_BASEPRICE', 'SKU','SKU_WHOLESALE','SKU_FREE']
        final_sales = final_sales[final_sales['SKU_BASEPRICE']>=0]


        final_sales = pd.merge(final_sales,toponly,left_on='TRANSACTION_ID',right_on='TRANSACTION_ID',how='left')

        #get sum of item prices for allocation
        def scalebaseprice(row): return row['SKU_QTY']*row['SKU_BASEPRICE']
        final_sales['SKU_QTY_BASEPRICE'] = final_sales.apply(scalebaseprice,axis=1)
        allocator = final_sales[['TRANSACTION_ID','SKU_QTY_BASEPRICE']].groupby(['TRANSACTION_ID']).sum().reset_index()
        allocator.columns = ['TRANSACTION_ID','SUMOF_SKU_QTY_BASEPRICE']
        final_sales = pd.merge(final_sales,allocator,left_on='TRANSACTION_ID',right_on='TRANSACTION_ID')

        def allocate_discount(row):
            if row['SKU_QTY_BASEPRICE']==0: return 0
            else: return row['TRANS_DISCOUNT_AMOUNT']*(row['SKU_QTY_BASEPRICE']/row['SUMOF_SKU_QTY_BASEPRICE'])
        final_sales['SKU_DISCOUNT'] = final_sales.apply(allocate_discount,axis=1)

        def allocate_taxes(row):
            if row['SKU_QTY_BASEPRICE']==0: return 0.0
            else: return row['TRANS_TAXES']*(row['SKU_QTY_BASEPRICE']/row['SUMOF_SKU_QTY_BASEPRICE'])
        final_sales['SKU_TAX'] = final_sales.apply(allocate_taxes,axis=1)

        def allocate_shipping(row):
            shipping_fee_per_order = 10
            if (row['SHIPPING_METHOD']=='Free Shipping')&(row['SKU_QTY_BASEPRICE']>0):
                return shipping_fee_per_order*(row['SKU_QTY_BASEPRICE']/row['SUMOF_SKU_QTY_BASEPRICE'])
            else: return 0.0
        final_sales['SKU_SHIPPING_COST'] = final_sales.apply(allocate_shipping,axis=1)

        def revenue(row):
            return row['SKU_QTY_BASEPRICE']-row['SKU_DISCOUNT']-row['SKU_SHIPPING_COST']
        final_sales['SKU_REVENUE'] = final_sales.apply(revenue,axis=1)

        def applyfract_nominal(row):
            if row['SKU_QTY_BASEPRICE']==0: return 0
            else:
                try:return np.rint(100*row['SKU_DISCOUNT']/row['SKU_QTY_BASEPRICE'])
                except ValueError: return 0
        final_sales['NOMINAL_PERC_DISCOUNT'] = final_sales.apply(applyfract_nominal,axis=1)

        def applyfract_actual(row):
            if row['SKU_QTY_BASEPRICE']==0: return 0
            else:
                try: return np.rint(100*(row['SKU_DISCOUNT']+row['SKU_SHIPPING_COST'])/row['SKU_QTY_BASEPRICE'])
                except ValueError: return 0
        final_sales['ACTUAL_PERC_DISCOUNT'] = final_sales.apply(applyfract_actual,axis=1)



        def apply_customer_filters_to_sales(final_sales_cust):

            # remove blank emails
            #final_sales_cust['EMAIL'] = final_sales_cust['EMAIL'].apply(lambda x: str(x).strip())
            final_sales_cust['EMAIL'] = final_sales_cust['EMAIL'].fillna('NotRecorded')
            final_sales_cust = final_sales_cust[final_sales_cust['EMAIL'] != 'NotRecorded']

            #remove internal employees
            def remove_internal(row):
                internal = 0
                emaillower = str(row['EMAIL']).strip()
                if (emaillower.find('@x3embrands')>-1)|(emaillower.find('@tropicsport')>-1)|(emaillower.find('@tropicsurf')>-1): return 1

                if emaillower.find("jerandall@contentfusion.net")>-1: return 1
                if emaillower == "randall.je@gmail.com": return 1
                if emaillower == "pallavir13@yahoo.com": return 1
                if emaillower == "palmeranthony@msn.com": return 1
                if emaillower == "amy.june@sbcglobal.net.com": return 1
                if row['SHIPPING_STREET'] == '6015 Lupton Drive': return 1
                return 0
            final_sales_cust['internal'] = final_sales_cust.apply(remove_internal,axis=1)
            final_sales_cust=final_sales_cust[final_sales_cust['internal']==0]
            final_sales_cust=final_sales_cust.drop('internal',axis=1)
            return final_sales_cust
        final_sales = apply_customer_filters_to_sales(final_sales)

        final_sales = final_sales.drop(['TRANS_DISCOUNT_AMOUNT','TRANS_TAXES','SHIPPING_STREET','SKU_QTY_BASEPRICE','SUMOF_SKU_QTY_BASEPRICE','SKU_DISCOUNT','SKU_TAX'],axis=1)
        final_sales.to_csv('MasterTables/TRANSACTION_MASTER.csv',index=False)

    def create_product_master_from_sales(self,raw_sales_df):
        unique_products = raw_sales_df[['Lineitem sku','Lineitem name']].drop_duplicates()

        #Create H1 Hierarchy:  SUNSCREEN, MOISTURIZER, LOTION, BODYWASH, CLEANSER, SCRUB, LIPBALM, BUNDLE,OTHER
        def assign_h1_product_hierarchy(row):
            text_descriptor = str(row['Lineitem name']).lower()
            h1 = "OTHER"
            if text_descriptor.find("sunscreen")>-1: h1 = "SUNSCREEN"
            if text_descriptor.find("moist") > -1: h1 = "MOISTURIZER"
            if text_descriptor.find("lotion") > -1: h1 = "LOTION"
            if text_descriptor.find("cooling") > -1: h1 = "LOTION"
            if text_descriptor.find("wash") > -1: h1 = "BODYWASH"
            if text_descriptor.find("clean") > -1: h1 = "CLEANSER"
            if text_descriptor.find("scrub") > -1: h1 = "SCRUB"
            if text_descriptor.find("lip") > -1: h1 = "BALM"
            if text_descriptor.find("bundle") > -1: h1 = "BUNDLE"
            return h1
        unique_products['Product_H1'] = unique_products.apply(assign_h1_product_hierarchy,axis=1)

        #CREATE H0 Hiearchy:  SUNPROTECTION, CLEANSING, DRYNESS, BUNDLE, OTHER
        def assign_h0_product_hierarchy(row):
            h1 = row['Product_H1']
            h0 = "OTHER"
            if h1 == 'SUNSCREEN': h0='SUNPROTECTION'
            if h1 == 'MOISTURIZER': h0='DRYNESS'
            if h1 == 'LOTION': h0 = 'DRYNESS'
            if h1 == 'BODYWASH': h0 = 'CLEANSING'
            if h1 == 'CLEANSER': h0 = 'CLEANSING'
            if h1 == 'SCRUB': h0 = 'CLEANSING'
            if h1 == 'BUNDLE': h0 = 'BUNDLE'
            if h1 == 'OTHER': h0 = 'OTHER'
            return h0
        unique_products['Product_H0'] = unique_products.apply(assign_h0_product_hierarchy, axis=1)

        #Extract Size

        #Pricing is handled separately in Sales Transaction Master


        #apply overrides
        #For H0


        #Force to be unique on SKU to avoid duplicating sales
        unique_products_final = unique_products.groupby(['Lineitem sku']).first().reset_index()
        #consider replacing first with either a list or dynamic most sales $ computation

        unique_products_final = unique_products_final[unique_products_final['Lineitem sku']!='']

        unique_products_final=unique_products_final [['Product_H0','Product_H1','Lineitem sku','Lineitem name']]
        unique_products_final=unique_products_final .sort_values(by=['Product_H0','Product_H1','Lineitem sku','Lineitem name'],ascending=[1,1,1,1])
        unique_products_final.columns = ['Product_H0','Product_H1','UNIQUE_SKU','COMMON_TEXT_DESC']
        unique_products_final.to_csv('MasterTables/ProductMaster.csv',index=False)

    def create_event_master_from_manual(self):
        #to join to sales, need columns:  Email, Product, Date (enumerate over all dates), Promo Name, Promo Attributes

        #digitize event tab
        eventlist = pd.read_excel('Raw/Event_MasterTable.xlsx',sheetname='Event')
        def cleanstartdate(row):
            s = row['Start Date']
            try:
                s = pd.to_datetime(s,errors='coerce')
                s = s.strftime('%Y-%m-%d')
            except ValueError: s = '1/1/2000'
            return s
        eventlist['Start Date'] = eventlist.apply(cleanstartdate,axis=1)
        def cleanenddate(row):
            s = row['End Date']
            if s == None:
                s = pd.to_datetime(row['Start Date'],errors='coerce')
                s =  s + pd.DateOffset(days=7)
                s = s.strftime('%Y-%m-%d')
            else:
                try:
                    s = pd.to_datetime(s,errors='coerce')
                    s = s.strftime('%Y-%m-%d')
                except ValueError:
                    s = pd.to_datetime(row['Start Date'], errors='coerce')
                    s = s + pd.DateOffset(days=7)
                    s = s.strftime('%Y-%m-%d')
            return s
        eventlist['End Date'] = eventlist.apply(cleanenddate,axis=1)

        eventlist = eventlist[eventlist['Start Date']!='1/1/2000']
        #stretch over all dates
        def stretch_dates(row):
            d = pd.date_range(start=row['Start Date'], end=row['End Date'],freq='D')
            d=d.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
            d=str(d)
            return d
        eventlist['datelist'] = eventlist.apply(stretch_dates,axis=1)

        from ast import literal_eval
        eventlist['datelist'] = eventlist['datelist'].apply(literal_eval)

        column_to_explode = 'datelist'
        res = (eventlist
               .set_index([x for x in eventlist.columns if x != column_to_explode])[column_to_explode]
               .apply(pd.Series)
               .stack()
               .reset_index())
        res = res.rename(columns={
            res.columns[-2]: 'exploded_{}_index'.format(column_to_explode),
            res.columns[-1]: '{}_exploded'.format(column_to_explode)})

        eventlist=res

        print("CREATMASTER A: "+str(len(eventlist[['Event']].drop_duplicates())))
        #print(eventlist[['Event']].drop_duplicates())

        #digitize customer list tab
        customerlist = pd.read_excel('Raw/Event_MasterTable.xlsx',sheetname='Customer List')
        for i in range(len(customerlist.columns)):
            addition_promoname = []
            addition_email = []
            focuspromo = customerlist[[customerlist.columns[i]]].as_matrix()
            for j in range(len(focuspromo)):
                addition_promoname.append(customerlist.columns[i])
                addition_email.append(focuspromo[j])
            addition_zip = pd.DataFrame({'EVENT_NAME': addition_promoname,'EMAIL': addition_email})
            if i==0: master_events_final = addition_zip
            else: master_events_final = pd.concat([master_events_final,addition_zip])

        master_events_final['EMAIL']=master_events_final['EMAIL'].apply(lambda x: str(x).replace('\'','').replace('[','').replace(']',''))
        master_events_final=master_events_final.drop_duplicates()

        print("CREATMASTER B1: "+str(len(eventlist[['Event']].drop_duplicates())))
        print("CREATMASTER B2: "+str(len(master_events_final[['EVENT_NAME']].drop_duplicates())))
        events_and_emails = pd.merge(eventlist,master_events_final,left_on=['Event'],right_on=['EVENT_NAME'],how='left')
        print("CREATMASTER B: "+str(len(events_and_emails[['Event']].drop_duplicates())))
        events_and_emails.to_csv('test1.csv',index=False)


        # digitize event products
        productlist = pd.read_excel('Raw/Event_MasterTable.xlsx', sheetname='Product List')[['Event Name','Product SKU']].drop_duplicates()
        productlist.columns = ['EVENT_NAME','PRODUCT']

        #product list can be difficult to scrub of blanks because excel gives them characters
        productlist = productlist[(productlist['PRODUCT']!=None)&(productlist['PRODUCT']!='')]
        productlist = productlist.dropna()

        #explode for Product == All in raw master
        all_products = pd.read_csv('MasterTables/PRODUCT_MASTER.csv')[['SKU']].drop_duplicates()
        all_products['DUMMY']='All'
        productlist = pd.merge(productlist,all_products,left_on='PRODUCT',right_on='DUMMY',how='inner')
        def overwriteproduct(row):
            if row['DUMMY'] == 'All': return row['SKU']
            else: return row['PRODUCT']
        productlist['PRODUCT'] = productlist.apply(overwriteproduct,axis=1)
        productlist = productlist.drop(['DUMMY','SKU'],axis=1)

        print('CREATMASTER B3: '+str(len(events_and_emails[['Event']].drop_duplicates())))
        print('CREATMASTER B3: '+str(len(productlist[['EVENT_NAME']].drop_duplicates())))
        event_emails_products = pd.merge(events_and_emails,productlist,left_on='Event',right_on='EVENT_NAME',how='inner')  #will break downstream otherwise
        print('CREATMASTER C: '+str(len(event_emails_products[['Event']].drop_duplicates())))

        event_emails_products = event_emails_products.drop(['EVENT_NAME_x','EVENT_NAME_y'],axis=1)

        event_emails_products.to_csv('MasterTables/EVENT_MASTER_VERBOSE.csv',index=False)

        keepcols = event_emails_products.columns
        keepcols = keepcols.drop('EMAIL').tolist()
        print('CREATMASTER D: '+str(len(event_emails_products[['Event']].drop_duplicates())))
        event_emails_products = event_emails_products.fillna('UNKNOWN')
        events_products = event_emails_products.groupby(keepcols).count().reset_index()  #count how many email recipients event had
        print('CREATMASTER D1: '+str(len(events_products[['Event']].drop_duplicates())))
        #events_products = event_emails_products.drop(['EMAIL'],axis=1).drop_duplicates()

        def promolength(row):
            d = (pd.to_datetime(row['End Date'])-pd.to_datetime(row['Start Date'])).days
            print(str(row['End Date'])+" "+str(row['Start Date'])+" "+str(d))
            return d
        events_products['EVENT_LENGTH'] = events_products.apply(promolength,axis=1)

        print('CREATMASTER F: '+str(len(events_products[['Event']].drop_duplicates())))
        events_products.to_csv('MasterTables/EVENT_MASTER_CONCISE.csv',index=False)


        #combine sales and events
        sales_df = pd.read_csv('MasterTables/TRANSACTION_MASTER.csv')
        sales_df['CREATED_TIMESTAMP'] = sales_df['CREATED_TIMESTAMP'].apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))
        #make sure no duplicates (sometimes rapid fire emails make multiple events apply to each day)
        event_products= pd.DataFrame(event_emails_products[['PRODUCT','datelist_exploded','Event']].groupby(['PRODUCT','datelist_exploded'])['Event'].apply(list))
        event_products= event_products.reset_index()
        sales_and_events = pd.merge(sales_df,event_products,left_on=['SKU','CREATED_TIMESTAMP'],right_on=['PRODUCT','datelist_exploded'],how='left')
        def countevents(row):
            if (row['Event'] == None)|(str(row['Event']) == 'nan'): return 0
            try:
                e = str(row['Event']).strip()
                if len(e)<3: e = '[]'
                e = ast.literal_eval(e)
                e = len(e)
                return e
            except ValueError:
                print("ERROR: "+str(e))
        sales_and_events['NUMEVENTS'] = sales_and_events.apply(countevents,axis=1)

        sales_and_events.to_csv('sales_and_events.csv',index=False)


        #combine sales and events and email
        conversions = pd.merge(sales_df,event_emails_products,left_on=['CREATED_TIMESTAMP','SKU','EMAIL'],right_on=['datelist_exploded','PRODUCT','EMAIL'],how='inner')
        conversions = conversions[['Event','PRODUCT','Code','SKU_QTY']].groupby(['Event','Code','PRODUCT']).sum().reset_index()
        conversions.to_csv('EVENT_CONVERSIONS.csv',index=False)

    def resolve_customer(self,raw_shopify):
        customer_candidates = raw_shopify[['EMAIL','Billing Name']].drop_duplicates()

        #make list of all email x billing name combinations where not 1:1
        mult_billingname = raw_shopify[['EMAIL','Billing Name']].groupby(['EMAIL']).count().reset_index()
        mult_billingname.columns = ['EMAIL','NumBillings']
        mult_billingname = mult_billingname[mult_billingname['NumBillings']>1]
        mult_billingname = pd.merge(mult_billingname,raw_shopify[['EMAIL','Billing Name']].drop_duplicates())
        print(mult_billingname)

        #creates a list of all emails that have multiple billing names



        #find billing_names with multiple emails



    def create_customer_master_from_sales(self,raw_sales):

        #remove invalid transactions
        goodtrans = pd.read_csv('MasterTables/TRANSACTION_MASTER.csv')[['TRANSACTION_ID','WHOLESALE','FREE']]
        goodtrans=goodtrans[goodtrans['WHOLESALE']==0]
        goodtrans = goodtrans[goodtrans['FREE'] == 0]
        raw_sales=pd.merge(raw_sales,goodtrans[['TRANSACTION_ID']],left_on=['Name'],right_on=['TRANSACTION_ID'])

        billinglist = ['Billing Name','Billing Street','Billing Address1','Billing Address2','Billing Company','Billing City','Billing Zip','Billing Province','Billing Country','Billing Phone']
        shiplist = ['Shipping Name','Shipping Street','Shipping Address1','Shipping Address2','Shipping Company','Shipping City','Shipping Zip','Shipping Province','Shipping Country','Shipping Phone']

        varlist = ['Email']+billinglist+shiplist
        raw_sales = raw_sales[varlist]
        raw_sales = raw_sales.rename(columns = {'Email':'EMAIL'})


        #replace first with tolist
        customer_desc = raw_sales.groupby(['EMAIL']).first().reset_index()

        a = Analysis
        purchase_desc = a.append_customer_sales_stats(self,'Hier1_Type')
        customer_desc = pd.merge(customer_desc,purchase_desc,left_on=['EMAIL'],right_on=['EMAIL'],how='left')

        customer_desc.to_csv('MasterTables/CUSTOMER_MASTER.csv',index=False)

class Analysis():

    def expand_dates(self,mindate,maxdate):
        if mindate != maxdate:
            tempdf = pd.DataFrame([mindate,maxdate])
            tempdf.columns = ['DATES']
            tempdf.index = tempdf['DATES']
            tempdf = tempdf.asfreq('D')
            tempdf.columns = ['DELETE']
            tempdf = tempdf.reset_index()
            tempdf = tempdf[['DATES']]
        else:
            tempdf = pd.DataFrame([mindate])
            tempdf.columns = ['DATES']
            tempdf.index = tempdf['DATES']
        return tempdf

    def detect_all_events_in_date_range(self,mindate,maxdate,specproduct='ALL'):
        events = pd.read_csv('MasterTables/EVENT_MASTER_CONCISE.csv')[['Event','datelist_exploded','PRODUCT']].drop_duplicates()
        if specproduct != 'All': events = events[events['PRODUCT']==specproduct]

        tempdf = self.expand_dates(self,mindate,maxdate)


        events['datelist_exploded']=events['datelist_exploded'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        tempdf['DATES']=tempdf['DATES'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        overlap = pd.merge(tempdf,events,left_on=['DATES'],right_on=['datelist_exploded'])
        overlap = overlap[['Event']].drop_duplicates()
        return overlap


    def filter_for_sales_in_prepostperiod(self,sales_df,rawevents,eventname,days=7,preorpostorduring = 'DURING',specproduct='ALL'): #days is the number of days to look back or forward

        rawevents = rawevents[rawevents['Event']==eventname]
        rawevents = rawevents[['Event','Start Date','End Date']].drop_duplicates()

        if preorpostorduring=='PRE':
            maxdate = pd.to_datetime(rawevents['Start Date'].iloc[0]) - pd.DateOffset(days=1)
            mindate = maxdate - pd.DateOffset(days=days)

        elif preorpostorduring == 'POST':
            mindate = pd.to_datetime(rawevents['End Date'].iloc[0]) + pd.DateOffset(days=1)
            maxdate = mindate + pd.DateOffset(days=days)
        else:
            mindate = pd.to_datetime(rawevents['Start Date'].iloc[0])
            maxdate = pd.to_datetime(rawevents['End Date'].iloc[0])

        if mindate.strftime('%Y-%m-%d')==maxdate.strftime('%Y-%m-%d'):  #one day events
            tempdf = pd.DataFrame([mindate])
            tempdf.columns = ['DATES']

        if mindate!=maxdate:
            tempdf = self.expand_dates(self,mindate,maxdate)

        if specproduct!='ALL':
            sales_df = sales_df[sales_df['SKU']==specproduct]


        try: nominal_med_discount = int(sales_df[sales_df['SKU_FREE']==0]['NOMINAL_PERC_DISCOUNT'].quantile(0.9))
        except ValueError:  nominal_med_discount = 0
        try: actual_med_discount = int(sales_df[sales_df['SKU_FREE']==0]['ACTUAL_PERC_DISCOUNT'].quantile(0.9))
        except ValueError: actual_med_discount = 0

        sales_df['CREATED_TIMESTAMP'] = sales_df['CREATED_TIMESTAMP'].apply(lambda x: pd.to_datetime(str(x)))
        sales_df = sales_df[['CREATED_TIMESTAMP', 'SKU_QTY','SKU_REVENUE']].groupby(['CREATED_TIMESTAMP']).sum().reset_index()

        sales_df['CREATED_TIMESTAMP'] = sales_df['CREATED_TIMESTAMP'].apply(lambda x: x.strftime('%Y-%m-%d'))
        tempdf['DATES'] = tempdf['DATES'].apply(lambda x: x.strftime('%Y-%m-%d'))
        sales_in_control_period = pd.merge(sales_df,tempdf,left_on=['CREATED_TIMESTAMP'],right_on=['DATES'])


        #append list of events that contaminate the period
        eventlist = self.detect_all_events_in_date_range(self,mindate,maxdate,specproduct).as_matrix()


        return sales_in_control_period,eventlist,nominal_med_discount,actual_med_discount

    def analyze_promos(self):
        a = Analysis
        raw_sales = pd.read_csv('MasterTables/TRANSACTION_MASTER.csv')
        raw_events = pd.read_csv('MasterTables/EVENT_MASTER_CONCISE.csv',encoding='latin1')

        print("START COUNT OF PROMOS: "+str(len(raw_events[['Event']].drop_duplicates())))

        listofevents = raw_events[['Event']].drop_duplicates()
        isfirst = True

        periodlength = 7  #the number of days before/after a promo to find baseline

        for i in range(len(listofevents)):
        #for i in range(3):
            eventname = str(listofevents.as_matrix()[i]).replace('[','').replace(']','').replace("\'",'')

            #get product list for event
            specific_event = raw_events[raw_events['Event']==eventname]
            listofproducts = specific_event[['PRODUCT']].drop_duplicates()

            def promolength(row):
                d = (pd.to_datetime(row['End Date'])-pd.to_datetime(row['Start Date'])).days+1
                return d
            try:
                specific_event['EVENT_LENGTH'] = specific_event.apply(promolength,axis=1)
                promolength = int(specific_event[['EVENT_LENGTH']].drop_duplicates().iloc[0])
            except ValueError:  promolength = 7 #if no specified end date, leave as 7

            try:
                startdate = str(specific_event[['Start Date']].drop_duplicates().values[0]).replace('[','').replace(']','').replace('\'','')
                enddate = str(specific_event[['End Date']].drop_duplicates().values[0]).replace('[','').replace(']','').replace('\'','')

                try:num_audience = float(str(specific_event[['# of Audience']].drop_duplicates().values[0]).replace('[','').replace(']','').replace('\'',''))
                except ValueError: num_audience=-1

                content = str(specific_event[['Content']].drop_duplicates().values[0]).replace('[','').replace(']','').replace('\'','')
                code = str(specific_event[['Code']].drop_duplicates().values[0]).replace('[','').replace(']','').replace('\'','')
            except IndexError: startdate = 'ERROR'; enddate='ERROR'; num_audience='ERROR'; content='ERROR'; code='ERROR'

            try:
                try:
                    for j in range(len(listofproducts)):
                        specproduct = str(listofproducts.as_matrix()[j]).replace('[','').replace(']','').replace("\'",'')

                        during_sales_df, duringeventlist, during_sales_discount_nominal,during_sales_discount_actual = a.filter_for_sales_in_prepostperiod(self, raw_sales,raw_events, eventname, periodlength, 'DURING',specproduct)
                        during_sales_qty = during_sales_df['SKU_QTY'].sum()
                        during_sales_rev = during_sales_df['SKU_REVENUE'].sum()
                        pre_sales_df, preeventlist, pre_sales_discount_nominal,pre_sales_discount_actual = a.filter_for_sales_in_prepostperiod(self, raw_sales,raw_events, eventname, periodlength, 'PRE',specproduct)
                        pre_sales_qty = pre_sales_df['SKU_QTY'].sum()
                        pre_sales_rev = pre_sales_df['SKU_REVENUE'].sum()
                        post_sales_df, posteventlist, post_sales_discount_nominal, post_sales_discount_actual = a.filter_for_sales_in_prepostperiod(self, raw_sales,raw_events, eventname, periodlength, 'POST',specproduct)
                        post_sales_qty = post_sales_df['SKU_QTY'].sum()
                        post_sales_rev = post_sales_df['SKU_REVENUE'].sum()

                        #make daily
                        if (periodlength>0)&(promolength>0):
                            during_sales_daily_qty = during_sales_qty/promolength
                            during_sales_daily_rev = during_sales_rev/promolength
                            pre_sales_daily_qty=pre_sales_qty/periodlength
                            pre_sales_daily_rev=pre_sales_rev/periodlength
                            post_sales_daily_qty=post_sales_qty/periodlength
                            post_sales_daily_rev=post_sales_rev/periodlength
                            if pre_sales_daily_qty+post_sales_daily_qty <= 0:
                                naive_lift_qty = 0
                                naive_lift_rev = 0
                            else:
                                naive_lift_qty = (during_sales_daily_qty/((pre_sales_daily_qty+post_sales_daily_qty)/2))-1
                                naive_lift_rev = (during_sales_daily_rev/((pre_sales_daily_rev+post_sales_daily_rev)/2))-1
                            print(eventname+", "+ str(specproduct) +", Pre: "+str(pre_sales_daily_rev)+", During: "+str(during_sales_daily_rev)+", Post: "+str(post_sales_daily_rev)+", $Rev Lift: "+str(100*naive_lift_rev)+"%")

                            add_to_list = [eventname,startdate,enddate,promolength,during_sales_discount_nominal,during_sales_discount_actual,pre_sales_discount_nominal,pre_sales_discount_actual,post_sales_discount_nominal,post_sales_discount_actual,num_audience,content,code,specproduct,pre_sales_daily_qty,during_sales_daily_qty,post_sales_daily_qty,naive_lift_qty,pre_sales_daily_rev,during_sales_daily_rev,post_sales_daily_rev,naive_lift_rev,preeventlist,duringeventlist,posteventlist]
                            cols = ['EVENT_NAME','START_DATE','END_DATE','EVENT_LENGTH','DURING_NOMINAL_DISCOUNT','DURING_ACTUAL_DISCOUNT','PRE_NOMINAL_DISCOUNT','PRE_ACTUAL_DISCOUNT','POST_NOMINAL_DISCOUNT','POST_ACTUAL_DISCOUNT','NUMAUDIENCE','CONTENT','CODE','PRODUCT','PREPERIOD_DAILY_QTY','DURING_DAILY_QTY','POSTPERIOD_DAILY_QTY','QTY_NAIVE_LIFT_PERC','PREPERIOD_DAILY_REV','DURING_DAILY_REV','POSTPERIOD_DAILY_REV','REV_NAIVE_LIFT_PERC','PRE_EVENTS','DURING_EVENTS','POST_EVENTS']

                            if (isfirst==True):
                                master_df = pd.DataFrame(add_to_list).transpose()
                                master_df.columns = cols
                                isfirst = False
                            else:
                                newdf = pd.DataFrame(add_to_list).transpose()
                                newdf.columns = cols
                                master_df = pd.concat([master_df,newdf])


                except IndexError: print("ERROR: "+eventname)
            except TypeError: print("DATES ERROR: " + eventname)
            master_df.to_csv('MasterTables/PROMO_MASTER.csv',index=False)



    def plot_sales_and_events(self,sales_df):
        sales_df['CREATED_TIMESTAMP'] = sales_df['CREATED_TIMESTAMP'].apply(lambda x:  pd.to_datetime(str(x)))
        sales_series = sales_df[['CREATED_TIMESTAMP','BASEPRICE']].groupby(['CREATED_TIMESTAMP']).sum().reset_index()

        #make timeseries for number of events active
        numeventsperday = sales_df[['CREATED_TIMESTAMP','NUMEVENTS']].groupby(['CREATED_TIMESTAMP'])['NUMEVENTS'].nunique().reset_index()
        sales_series = pd.merge(sales_series,numeventsperday,left_on='CREATED_TIMESTAMP',right_on='CREATED_TIMESTAMP',how='left')

        #fill in days with zero sales
        a = sales_series[['BASEPRICE','NUMEVENTS']]
        a.index = sales_series['CREATED_TIMESTAMP']
        alldays =a.asfreq('D')
        alldays = alldays.fillna(0)
        sales_series = pd.DataFrame(alldays)
        sales_series=sales_series.reset_index()
        sales_series.columns = ['DATE','REVENUE',"NUMEVENTS"]

        fig, ax1 = plt.subplots()
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Revenue')
        ax1.plot(sales_series.DATE, sales_series.REVENUE,color='tab:green')
        ax1.tick_params(axis='y')

        ax2 = ax1.twinx()
        ax2.set_ylabel('NUMEVENTS')  # we already handled the x-label with ax1
        ax2.plot(sales_series.DATE, sales_series.NUMEVENTS)
        ax2.tick_params(axis='y')

        plt.title("Sales and Simultaneous Event Count by Date")
        fig.tight_layout()  # otherwise the right y-label is slightly clipped
        plt.show()
        sales_series.to_csv('sales_series.csv',index=False)


    def append_customer_sales_stats(self,pivcol='NONE'):
        sales_data = pd.read_csv('MasterTables/TRANSACTION_MASTER.csv')
        sales_data = pd.merge(sales_data, pd.read_csv('MasterTables/PRODUCT_MASTER.csv'), left_on='SKU', right_on='SKU')

        if pivcol=='NONE':sales_data = sales_data[['TRANSACTION_ID','EMAIL','CREATED_TIMESTAMP']].drop_duplicates()
        else: sales_data = sales_data[['TRANSACTION_ID', 'EMAIL', 'CREATED_TIMESTAMP',pivcol]].drop_duplicates()

        sales_data_piv = sales_data.groupby(['EMAIL','TRANSACTION_ID']).count().reset_index()[['EMAIL','TRANSACTION_ID']]
        tot_recurrence_piv = sales_data_piv.groupby('EMAIL').count().reset_index()

        #Get last transaction
        sales_data['CREATED_TIMESTAMP'] = sales_data['CREATED_TIMESTAMP'].apply(lambda x: pd.to_datetime(x))
        last_trans = sales_data[['EMAIL','CREATED_TIMESTAMP']].groupby(['EMAIL']).max().reset_index()
        tot_recurrence_piv = pd.merge(tot_recurrence_piv,last_trans,left_on=['EMAIL'],right_on=['EMAIL'])
        tot_recurrence_piv.columns = ['EMAIL','TOT_TRANS_COUNT',"LAST_TRANS_DATE"]

        if pivcol != 'NONE':

            sales_data_piv = sales_data.groupby(['EMAIL', 'TRANSACTION_ID',pivcol]).count().reset_index()[['EMAIL', 'TRANSACTION_ID',pivcol]]
            pivcol_recurrence_piv = pd.pivot_table(sales_data_piv,index=['EMAIL'],columns=pivcol,values=['TRANSACTION_ID'],aggfunc='count').reset_index()
            cleancolnames = []
            for i in range(len(pivcol_recurrence_piv.columns)):
                startcol = str(pivcol_recurrence_piv.columns[i])
                if startcol.find('EMAIL')==-1:
                    startcol = "TRANSCOUNT_" + startcol
                    startcol = startcol.replace('(\'TRANSACTION_ID\', ', '').replace(')', '').replace('\'', '')
                else: startcol = 'EMAIL'
                cleancolnames.append(startcol)
            pivcol_recurrence_piv.columns = cleancolnames
            tot_recurrence_piv = pd.merge(tot_recurrence_piv,pivcol_recurrence_piv,left_on='EMAIL',right_on='EMAIL')
            tot_recurrence_piv =  tot_recurrence_piv.fillna(0.0)

        return tot_recurrence_piv




    def basket_analyis(self,sales_data, hierarchy_field):
        sales_data=sales_data[sales_data['SKU_WHOLESALE']==0]
        sales_data = sales_data[sales_data['SKU_FREE'] == 0]

        sales_data = sales_data[['TRANSACTION_ID','SKU']]
        product_master = pd.read_csv('MasterTables/PRODUCT_MASTER.csv')
        sales_data=pd.merge(sales_data,product_master,left_on=['SKU'],right_on=['SKU'])
        sales_data = sales_data[['TRANSACTION_ID',hierarchy_field]].drop_duplicates()
        sales_data.columns = ['TRANSACTION_ID','PRODGROUP']

        blowup = pd.merge(sales_data,sales_data,left_on='TRANSACTION_ID',right_on='TRANSACTION_ID')

        blowup = blowup.groupby(['PRODGROUP_x','PRODGROUP_y']).count().reset_index()
        blowup.columns = ['Ind_Item','Dep_Item','Cross_Count']

        ind = sales_data.groupby(['PRODGROUP']).count().reset_index()
        ind.columns = ['Ind_Item','Ind_Count']

        final_df = pd.merge(ind,blowup,left_on=['Ind_Item'],right_on=['Ind_Item'])
        final_df['Correlation'] = final_df['Cross_Count']/final_df['Ind_Count']

        final_df.to_csv('AnalyticalOutput/basket_correlations.csv',index=False)
        return final_df

    def basket_gap_opportunities(self,basket_analysis,sales_data,hierarchy_field):
        basket_analysis = basket_analysis[basket_analysis['Cross_Count']>5]
        basket_analysis = basket_analysis[basket_analysis['Correlation'] > 0.5]
        basket_analysis = basket_analysis[basket_analysis['Ind_Item'] != basket_analysis['Dep_Item']]

        product_master=pd.read_csv('MasterTables/PRODUCT_MASTER.csv')
        sales_data=pd.merge(sales_data,product_master,left_on=['SKU'],right_on=['SKU'])
        sales_data = sales_data[['EMAIL',hierarchy_field]]
        sales_data.columns=['EMAIL','PRODGROUP']
        sales_data_short = sales_data.groupby(['EMAIL','PRODGROUP']).count().reset_index()

        potential_opportunities = pd.merge(sales_data_short,basket_analysis,left_on=['PRODGROUP'],right_on=['Ind_Item'])


        #remove if purchased exact item
        opportunities = pd.merge(potential_opportunities,sales_data_short,left_on=['EMAIL','Dep_Item'],right_on=['EMAIL','PRODGROUP'],how='left')
        opportunities['HasPurchased'] = opportunities['PRODGROUP_y'].apply(lambda x: 0 if len(str(x))<=5 else 1)
        opportunities = opportunities[opportunities['HasPurchased']==0]
        opportunities = opportunities.drop(['PRODGROUP_x','PRODGROUP_y','HasPurchased'],axis=1)


        #add descriptors of dep sku (breaks if hierarchy_field is less granular than SKU because multiple SKUs match the hierarchy
        #opportunities = pd.merge(opportunities,product_master,left_on='Dep_Item',right_on=hierarchy_field)

        #add details about the customer's past purchases
        customer_sales_descriptors = self.append_customer_sales_stats(self,'Hier1_Type')
        opportunities = pd.merge(opportunities,customer_sales_descriptors,left_on='EMAIL',right_on='EMAIL')
        opportunities = pd.merge(opportunities, pd.read_csv('MasterTables/CUSTOMER_MASTER.csv',encoding='latin1')[['EMAIL','Billing Province']], left_on='EMAIL', right_on='EMAIL')

        opportunities.to_csv('AnalyticalOutput/gap_opportunities.csv',index=False)



def master_controller():
    warnings.filterwarnings("ignore")


    def create_mastertables():
        di = Data_Ingestion
        a = Analysis

        di.create_transaction_master_from_sales(di,pd.read_csv('Raw/orders_export_20180921.csv'))
        #di.create_customer_master_from_sales(di, pd.read_csv('Raw/orders_export_20180921.csv'))
        #di.resolve_customer("SELF",raw_shopify)
        #di.create_product_master_from_sales(di,clean_shopify)
        di.create_event_master_from_manual(di)

    def plot_sales():
        a = Analysis
        a.plot_sales_and_events(a,pd.read_csv('sales_and_events.csv'))

    def make_basket_fullsteps():
        di = Data_Ingestion
        a = Analysis

        prodgroup = 'Hier1_Type'
        basket_correlations = a.basket_analyis("SELF", pd.read_csv('MasterTables/TRANSACTION_MASTER.csv'),prodgroup)
        a.basket_gap_opportunities(a, basket_correlations, pd.read_csv('MasterTables/TRANSACTION_MASTER.csv'),prodgroup)

    #create_mastertables()
    #make_basket_fullsteps()
    #plot_sales()
    a = Analysis
    a.analyze_promos(a)

import tkinter
master_controller()
