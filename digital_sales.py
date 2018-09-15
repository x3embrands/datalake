import pandas as pd
import numpy as np
import warnings

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

        toponly=toponly[['Name','Financial Status','Paid at','Fulfillment Status','Currency','Discount Code','Shipping Method','Source']]
        toponly.columns = ['TRANSACTION_ID','FINANCIAL_STATUS','PAID_TIMESTAMPM','FULFILLMENT','CURRENCY','DISCOUNT_CODE','SHIPPING','SOURCE']

        #allocate tax
        #allocate shipping

        final_sales = raw_sales_df[['Name', 'Email', 'Created at', 'Lineitem quantity', 'Lineitem price', 'Lineitem sku','WHOLESALE','FREE']]
        final_sales.columns = ['TRANSACTION_ID', 'EMAIL', 'CREATED_TIMESTAMP', 'QTY', 'PRICE', 'SKU','WHOLESALE','FREE']

        final_sales = pd.merge(final_sales,toponly,left_on='TRANSACTION_ID',right_on='TRANSACTION_ID',how='left')

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
                return 0
            final_sales_cust['internal'] = final_sales_cust.apply(remove_internal,axis=1)
            final_sales_cust=final_sales_cust[final_sales_cust['internal']==0]
            final_sales_cust=final_sales_cust.drop('internal',axis=1)
            return final_sales_cust
        final_sales = apply_customer_filters_to_sales(final_sales)

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
            d=d.format(formatter=lambda x: x.strftime('%Y%m%d'))
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

        eventlist.to_csv('testevent.csv',index=False)

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

        master_events_final.to_csv('testcust.csv', index=False)

        events_and_emails = pd.merge(eventlist,master_events_final,left_on=['Event'],right_on=['EVENT_NAME'])
        events_and_emails.to_csv('testeventemail.csv',index=False)


        #handle products



    def resolve_customer(self,raw_shopify):
        customer_candidates = raw_shopify[['Email','Billing Name']].drop_duplicates()

        #make list of all email x billing name combinations where not 1:1
        mult_billingname = raw_shopify[['Email','Billing Name']].groupby(['Email']).count().reset_index()
        mult_billingname.columns = ['Email','NumBillings']
        mult_billingname = mult_billingname[mult_billingname['NumBillings']>1]
        mult_billingname = pd.merge(mult_billingname,raw_shopify[['Email','Billing Name']].drop_duplicates())
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

        #replace first with tolist
        customer_desc = raw_sales.groupby(['Email']).first().reset_index()

        customer_desc.to_csv('MasterTables/CUSTOMER_MASTER.csv',index=False)

class Analysis():

    def append_customer_sales_stats(self,pivcol='NONE'):
        sales_data = pd.read_csv('MasterTables/TRANSACTION_MASTER.csv')
        sales_data = pd.merge(sales_data, pd.read_csv('MasterTables/PRODUCT_MASTER.csv'), left_on='SKU', right_on='SKU')

        if pivcol=='NONE':sales_data = sales_data[['TRANSACTION_ID','EMAIL','CREATED_TIMESTAMP']].drop_duplicates()
        else: sales_data = sales_data[['TRANSACTION_ID', 'EMAIL', 'CREATED_TIMESTAMP',pivcol]].drop_duplicates()

        sales_data_piv = sales_data.groupby(['EMAIL','TRANSACTION_ID']).count().reset_index()[['EMAIL','TRANSACTION_ID']]
        tot_recurrence_piv = sales_data_piv.groupby('EMAIL').count().reset_index()
        tot_recurrence_piv.columns = ['EMAIL','TOT_TRANS_COUNT']

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
        sales_data=sales_data[sales_data['WHOLESALE']==0]
        sales_data = sales_data[sales_data['FREE'] == 0]

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

        opportunities.to_csv('AnalyticalOutput/gap_opportunities.csv',index=False)


def master_controller():
    warnings.filterwarnings("ignore")

    def create_mastertables():
        di = Data_Ingestion
        a = Analysis

        #di.create_transaction_master_from_sales(di,pd.read_csv('Raw/all_orders180827.csv'))
        #di.create_customer_master_from_sales(di, pd.read_csv('Raw/all_orders180827.csv'))
        #di.resolve_customer("SELF",raw_shopify)
        #a.append_recurrences("Self",raw_shopify)
        #di.create_product_master_from_sales(di,clean_shopify)
        di.create_event_master_from_manual(di)



    def make_basket_fullsteps():
        di = Data_Ingestion
        a = Analysis

        prodgroup = 'Hier1_Type'
        basket_correlations = a.basket_analyis("SELF", pd.read_csv('MasterTables/TRANSACTION_MASTER.csv'),prodgroup)
        a.basket_gap_opportunities(a, basket_correlations, pd.read_csv('MasterTables/TRANSACTION_MASTER.csv'),prodgroup)

    #make_basket_fullsteps()
    create_mastertables()
master_controller()