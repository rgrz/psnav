from unittest import result
import pandas as pd

pspnlfield_df = pd.read_csv('./data/pspnlfield.csv', sep=',')
pspnlgroup_df = pd.read_csv('./data/pspnlgroup.csv', sep=',')
pspcmprog_df = pd.read_csv('./data/pspcmprog.csv', sep=',')


def get_df_from_file(input_type, my_input):
    global pspnlgroup_df, pspnlfield_df

    cmp_pnl_df = pd.DataFrame() # Component, Page
    pnl_sub_df = pd.DataFrame() # Page, SubPage
    pnl_rec_df = pd.DataFrame() # Page, Record
    sub_rec_df = pd.DataFrame() # SubPage, Record
    sub_sub_df = pd.DataFrame() # SubPage, SubPage

    if input_type == 'Component':
        # cmp_pnl_df = fget_pages_in_component(my_input).drop_duplicates() # returns df [pnlgrpname, pnlname] takes Component str
        cmp_pnl_df = pspnlgroup_df[pspnlgroup_df['PNLGRPNAME'] == my_input][['PNLGRPNAME', 'PNLNAME']].drop_duplicates()
        pnl_sub_df = fget_subpages_in_page(cmp_pnl_df['PNLNAME']).drop_duplicates() # returns df [pnlname, subpnlname] takes dataframe['pnlname'] 
        # pnl_rec_df = fget_records_in_pages(cmp_pnl_df['PNLNAME']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        pnl_rec_df = pspnlfield_df[(pspnlfield_df['PNLNAME'].isin(cmp_pnl_df['PNLNAME'])) & (pspnlfield_df['RECNAME'] != ' ')][['PNLNAME','RECNAME']].drop_duplicates()
        # sub_rec_df = fget_records_in_subpages(pnl_sub_df['SUBPNLNAME']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['subpnlname']
        sub_rec_df = pspnlfield_df[(pspnlfield_df['PNLNAME'].isin(pnl_sub_df['SUBPNLNAME'])) & (pspnlfield_df['RECNAME'] != ' ')][['PNLNAME','RECNAME']].drop_duplicates()
    if input_type == 'Page':
        # cmp_pnl_df = fget_component_from_page(my_input).drop_duplicates()
        cmp_pnl_df = pspnlgroup_df[pspnlgroup_df['PNLNAME'] == my_input][['PNLGRPNAME', 'PNLNAME']].drop_duplicates()
        pnl_sub_df = fget_subpages_in_page(cmp_pnl_df['PNLNAME']).drop_duplicates() # returns df [pnlname, subpnlname] takes str
        # pnl_rec_df = fget_records_in_pages(cmp_pnl_df['PNLNAME']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        pnl_rec_df = pspnlfield_df[(pspnlfield_df['PNLNAME'].isin(cmp_pnl_df['PNLNAME'])) & (pspnlfield_df['RECNAME'] != ' ')][['PNLNAME','RECNAME']].drop_duplicates()
        # sub_rec_df = fget_records_in_subpages(pnl_sub_df['SUBPNLNAME']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        sub_rec_df = pspnlfield_df[(pspnlfield_df['PNLNAME'].isin(pnl_sub_df['SUBPNLNAME'])) & (pspnlfield_df['RECNAME'] != ' ')][['PNLNAME','RECNAME']]
    if input_type == 'Record':
        # First get Pages/Subpages (From pspnlfield)
        # pnl_rec_df = fget_pages_from_record(my_input)
        pnl_sub_df = pspnlfield_df[(pspnlfield_df['RECNAME'] == my_input)][['PNLNAME', 'RECNAME']].drop_duplicates()
        pnl_sub_df = pnl_sub_df[(~pspnlfield_df['PNLNAME'].isin(pspnlfield_df['SUBPNLNAME']))]

        # sub_rec_df = fget_subpages_from_record(my_input)
        sub_rec_df = pspnlfield_df[(pspnlfield_df['RECNAME'] == my_input)][['PNLNAME', 'RECNAME']].drop_duplicates()
        sub_rec_df = sub_rec_df[(pspnlfield_df['PNLNAME'].isin(pspnlfield_df['SUBPNLNAME']))]
        # drill up gets all pages subpages from a page/subpage series
        pnl_all_df = drillup_all_pages(pspnlfield_df, sub_rec_df['PNLNAME'])
        # select for only page-subpage edges
        pnl_sub_df = pnl_all_df[(pnl_all_df['PNLNAME'].isin(pnl_rec_df['PNLNAME'])) ][['PNLNAME', 'SUBPNLNAME']].drop_duplicates()
        # select for only subpage-subpage edges
        sub_sub_df = pnl_all_df[(~pnl_all_df['PNLNAME'].isin(pnl_rec_df['PNLNAME']))][['PNLNAME', 'SUBPNLNAME']].drop_duplicates()
        pnl_all_df = pd.concat([pnl_rec_df['PNLNAME'], pnl_sub_df['PNLNAME']], ignore_index=True)
        
        cmp_pnl_df = pspnlgroup_df[pspnlgroup_df['PNLNAME'].isin(pnl_all_df)][['PNLGRPNAME','PNLNAME']]
        
    # print(f"cmp_pnl_df.columns: {cmp_pnl_df.columns}\ncmp_pnl_df.columns: {pnl_sub_df.columns}\ncmp_pnl_df.columns: {pnl_rec_df.columns}\ncmp_pnl_df.columns: {sub_rec_df.columns}\ncmp_pnl_df.columns: {sub_sub_df.columns}\n")
    cmp_pnl_df.columns = ['pnlgrpname', 'pnlname']
    pnl_sub_df.columns = ['pnlname', 'subpnlname']
    pnl_rec_df.columns = ['pnlname', 'recname']
    sub_rec_df.columns = ['pnlname', 'recname']
    if sub_sub_df.size > 0:
        sub_sub_df.columns = ['pnlname', 'subpnlname']

    return cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df

# def fget_pages_in_component(component_name: str):
#     global pspnlgroup_df
#     # pspnlgroup_df = pd.read_csv('./data/pspnlgroup.csv', sep=',')
#     cmp_pnl_df = pspnlgroup_df[pspnlgroup_df['PNLGRPNAME'] == component_name][['PNLGRPNAME', 'PNLNAME']]

#     return cmp_pnl_df

# def fget_component_from_page(page_name: str):
#     global pspnlgroup_df
#     # pspnlgroup_df = pd.read_csv('./data/pspnlgroup.csv', sep=',')
#     cmp_pnl_df = pspnlgroup_df[pspnlgroup_df['PNLNAME'] == page_name][['PNLGRPNAME', 'PNLNAME']]

#     return cmp_pnl_df

# when input type is Record a drillup from pages
# def fget_component_from_pages(pages):
#     rec_sub_df = pspnlgroup_df[pspnlgroup_df['PNLNAME'].isin(pages)][['PNLGRPNAME','PNLNAME']]

#     return rec_sub_df


# temp_df = pd.DataFrame()
# result_df = pd.DataFrame(columns = ['PNLNAME', 'SUBPNLNAME'])
pnl_drilldown_df = pd.DataFrame()
# From a Series of SubPages get all the embedded SubPages 
def drilldown_subpage(main_df, page_s):
    global pnl_drilldown_df

    new_df = main_df[main_df['PNLNAME'].isin(page_s)]
    if new_df.size > 0:
        drilldown_subpage(main_df, new_df['SUBPNLNAME'])
        pnl_drilldown_df = pd.concat([pnl_drilldown_df, new_df], ignore_index=True)

    return

def drillup_all_pages(main_df, subpage_series):

    new_df = main_df[main_df['SUBPNLNAME'].isin(subpage_series)][['PNLNAME', 'SUBPNLNAME']]
    concat_df = pd.DataFrame()
    while new_df.size > 0:
        concat_df = pd.concat([concat_df, new_df], ignore_index=True)
        subpage_series = new_df['PNLNAME']
        new_df = main_df[main_df['SUBPNLNAME'].isin(subpage_series)][['PNLNAME', 'SUBPNLNAME']]# .drop_duplicates()
    
    return concat_df
    
# page series is a pd.Series cmp_pnl_df['PNLNAME'] with one or more elements
def fget_subpages_in_page(pages_series):
    global pspnlfield_df, pnl_drilldown_df
    pnl_drilldown_df = pd.DataFrame(columns = ['PNLNAME', 'SUBPNLNAME'])
    temp_df = pd.DataFrame()
    
    temp_df = pspnlfield_df[pspnlfield_df['SUBPNLNAME'] != ' '][['PNLNAME', 'SUBPNLNAME']].copy()
    # drills down the page tree and saves page-subpages edges into result_df
    drilldown_subpage(temp_df, pages_series)
    # print(f"temp_df: out of the recursive function:\n {sub_pnl_df}")
    # print(f"result_df: out of the recursive function:\n {result_df}")

    return pnl_drilldown_df

# def fget_records_in_pages(subpages_df):
#     rec_sub_df = pspnlfield_df[pspnlfield_df['RECNAME'].isin(subpages_df)][['PNLNAME','RECNAME']]

#     return rec_sub_df

# Get Records in a SubPage > SubPage::Record::Field edges
# def fget_records_in_subpages(pages_df):
#     rec_sub_df = pspnlfield_df[pspnlfield_df['RECNAME'].isin(pages_df)][['PNLNAME','RECNAME']]

#     return rec_sub_df

def fget_pages_from_record(recname: str):
    global pspnlfield_df
    # First see if there the record is present in a Page     
    # pnl_sub_df = pspnlfield_df[(pspnlfield_df['FIELDNAME'] != ' ')]
    pnl_sub_df = pspnlfield_df[(pspnlfield_df['RECNAME'] == recname)][['PNLNAME', 'RECNAME']].drop_duplicates()
    pnl_sub_df = pnl_sub_df[(~pspnlfield_df['PNLNAME'].isin(pspnlfield_df['SUBPNLNAME']))]

    return pnl_sub_df

# def fget_subpages_from_record(recname: str):
#     global pspnlfield_df
#     # sub_rec_df = pspnlfield_df[(pspnlfield_df['FIELDNAME'] != ' ')]
#     sub_rec_df = pspnlfield_df[(pspnlfield_df['RECNAME'] == recname)][['PNLNAME', 'RECNAME']].drop_duplicates()
#     sub_rec_df = sub_rec_df[(pspnlfield_df['PNLNAME'].isin(pspnlfield_df['SUBPNLNAME']))]

#     return sub_rec_df

# def fget_pages_from_subpages(subpage_series): 
#     # pspnlfield_df = pd.read_csv('./data/pspnlfield.csv', sep=',')
#     pspnlfield_df = drillup_subpage(subpage_series)
   
#     return pspnlfield_df

# page_subpage_df complete dataframe of pages/subpages
# subpage_series Series of Subpages to drill up
# Returns a list of tuples with SubPage/SubPage edges (not Page/SubPage edge)
# So all of them ought to be Subpages
# def drillup_page(subpage_series):
    
#     page_subpage_df = pspnlfield_df
#     edges = []
    
#     # for line in list find subpage as a page
#     for row in page_subpage_df.iterrows():
#         # if subpage is also a page drillup
#         for subpage in subpage_series:
#             if row['SUBPNLNAME'] == subpage:
#                 # subpage_nodes.append(line['PNLNAME'])
#                 edges.append((row['PNLNAME'],  row['SUBPNLNAME']))
#                 drillup_page(row['PNLNAME'])
        
#     return edges



# page_subpage_df complete dataframe of pages/subpages
# subpage_series Series of Subpages to drill up
# Returns a list of tuples with SubPage/SubPage edges (not Page/SubPage edge)
# So all of them ought to be Subpages
def drillup_subpage(subpage_series):
    global pspnlfield_df
    page_subpage_df = pspnlfield_df
    edges = []
    
    # for line in list find subpage as a page
    for row in page_subpage_df.iterrows():
        # if subpage is also a page drillup
        for subpage in subpage_series:
            if row['SUBPNLNAME'] == subpage:
                # subpage_nodes.append(line['PNLNAME'])
                edges.append((row['PNLNAME'],  row['SUBPNLNAME']))
                drillup_subpage(row['PNLNAME'])
        
    return edges

# Returns pd.DataFrame with 2 columns: Node and "Path to Event"
# obj_name is a Dictionary of the Node key (name) and value (label)
def fquery_peoplecode_events(qry_name, obj_name):
    global pspcmprog_df 

    # obj_name is a list of dictionaries (name, label)
    labels = []
    for i in obj_name:
        labels.append(i['label'])
    # print(f"obj_name :\n{labels}")
    # in_stmt = '\', \''.join(labels)
    # and_clause = f" and objectvalue1 in ('{in_stmt}')"

    # Component Events
    if qry_name == "qry_event_component": # Component.event (PreBuild, PostBuild, SavePostChange, SavePreChange) >> OBJECTID1 = 10, OBJECTID3 = 12 (Event)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  10) & (pspcmprog_df['OBJECTID3'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 
        
    elif qry_name == "qry_event_comp_rec": # Component.Record.event (RowInit, RowInsert, RowDelete, RowSelect, SaveEdit, SavePostChange, SavePreChange)  >> OBJECTID1 = 10, OBJECTID3 = 1 (Record) AND OBJECT4 = 12 (Event)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  10) & (pspcmprog_df['OBJECTID3'] == 1) & (pspcmprog_df['OBJECTID4'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 + '.' +  df_all.OBJECTVALUE4 
        
    elif qry_name == "qry_event_comp_rec_field": # Component.Record.Field.event (FieldChange, FieldDefault, FieldEdit, PrePopUp)  >> OBJECTID1 = 10, OBJECTID3 = 1 (Record), OBJECT4 = 2 (Field) AND OBJECT5 = 12 (Event)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  10) & (pspcmprog_df['OBJECTID3'] == 1) & (pspcmprog_df['OBJECTID4'] == 2) & (pspcmprog_df['OBJECTID5'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 + '.' +  df_all.OBJECTVALUE4  + '.' +  df_all.OBJECTVALUE5

    # Page Events
    elif qry_name == "qry_event_page": # Page.event (Activate) >> OBJECTID1 = 9, OBJECTID2 = 12
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  9) & (pspcmprog_df['OBJECTID2'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 

    # Record Events
    elif qry_name == "qry_event_record_field": # Record.event (All... FieldFormula, FieldChange, FieldDefault, FieldEdit, PrePopUp, RowInit, RowInsert, RowDelete, RowSelect, SaveEdit, SavePostChange, SavePreChange, Workflow, SearchSave, SearchInit) >> OBJECTID1 = 1, OBJECTID2 = 2 (Field), OBJECT3 = 12 (Event)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  1) & (pspcmprog_df['OBJECTID2'] == 2) & (pspcmprog_df['OBJECTID3'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 
        
    # Application Package Classes
    elif qry_name == "qry_appPckg_l1": # ApplicationPackage.Class.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 107 (class), OBJECTID3 = 12 (OnExecute)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  104) & (pspcmprog_df['OBJECTID2'] == 107) & (pspcmprog_df['OBJECTID3'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 

    elif qry_name == "qry_appPckg_l2": # ApplicationPackage.SubAppPckg.Class.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 105 (SubPackage), OBJECTID3 107 (class), OBJECTID4 = 12 (OnExecute)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  104) & (pspcmprog_df['OBJECTID2'] == 105) & (pspcmprog_df['OBJECTID3'] == 107) & (pspcmprog_df['OBJECTID4'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 + '.' +  df_all.OBJECTVALUE4

    elif qry_name == "qry_appPckg_l3": # ApplicationPackage.SubAppPckg.SubAppPckg.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 105 (SubPackage), OBJECTID3 = 106 (SubPackage), OBJECTID4 107 (class), OBJECTID5 = 12 (OnExecute)
        df_all = pspcmprog_df[(pspcmprog_df['OBJECTID1'] ==  104) & (pspcmprog_df['OBJECTID2'] == 105) & (pspcmprog_df['OBJECTID3'] == 106) & (pspcmprog_df['OBJECTID4'] == 107) & (pspcmprog_df['OBJECTID5'] == 12) & (pspcmprog_df['OBJECTVALUE1'].isin(labels))]
        df_all['node'] = df_all.OBJECTVALUE1
        df_all['eventpath'] = df_all.OBJECTVALUE1 + '.' +  df_all.OBJECTVALUE2 + '.' +  df_all.OBJECTVALUE3 + '.' +  df_all.OBJECTVALUE4 + '.' +  df_all.OBJECTVALUE5
    else:
        exit(0)
    
    return df_all[['node', 'eventpath']]
