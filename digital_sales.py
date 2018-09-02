import pandas as pd

class Data_Ingestion():

    def clean_shopify_data(self):
        raw_shopify = pd.read_csv('Raw/all_orders180827.csv')

        #this eliminates multipurchase events
        raw_shopify = raw_shopify[raw_shopify['Financial Status']=='paid']

        raw_shopify['Email']=raw_shopify['Email'].fillna('NotRecorded')
        raw_shopify['Billing Name'] = raw_shopify['Billing Name'].fillna('NotRecorded')
        raw_shopify.to_csv('cleaned_shopify.csv',index=False)
        return raw_shopify

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





class Analysis():

    def append_recurrences(self,sales_data):
        sales_data = sales_data[['Name','Email','Created at']].drop_duplicates()
        sales_data = sales_data[sales_data['Email'] != 'NotRecorded']
        #sales_data = sales_data.sort_values(by=['Email','Created_At'],ascending=True)
        sales_data_piv = sales_data.groupby(['Email']).count().reset_index()
        count_piv = sales_data_piv.groupby('Name').count()
        print(count_piv)

        listofcustomers = sales_data_piv[['Email']].drop_duplicates()
        listofcustomers = pd.merge(listofcustomers,sales_data_piv,left_on=['Email'],right_on=['Email'])
        listofcustomers.to_csv('multicustomers.csv',index=False)

    def basket_analyis(self,sales_data):
        sales_data = sales_data[['Name','Lineitem name']]
        blowup = pd.merge(sales_data,sales_data,left_on='Name',right_on='Name')

        blowup = blowup.groupby(['Lineitem name_x','Lineitem name_y']).count().reset_index()
        blowup.columns = ['Ind_Item','Dep_Item','Cross_Count']

        ind = sales_data.groupby(['Lineitem name']).count().reset_index()
        ind.columns = ['Ind_Item','Ind_Count']

        final_df = pd.merge(ind,blowup,left_on=['Ind_Item'],right_on=['Ind_Item'])
        final_df['Correlation'] = final_df['Cross_Count']/final_df['Ind_Count']

        final_df.to_csv('basket_analysis.csv',index=False)

def master_controller():
    di = Data_Ingestion
    #raw_shopify = di.clean_shopify_data("SELF")
    #di.resolve_customer("SELF",raw_shopify)
    a = Analysis
    #a.append_recurrences("Self",raw_shopify)
    a.basket_analyis("SELF",pd.read_csv('Raw/all_orders180827.csv'))
master_controller()