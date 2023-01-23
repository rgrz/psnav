# Oracle connection to Vanilla PeopleSoft
import cx_Oracle
import sqlalchemy

import pandas as pd

global ora_version, query_sql, engine

cxora_ini = True

# # Initaliaze the Python cx Oracle Library
def init_ora():
    global cxora_ini, engine, conn
    try: 
        # ora_version = cx_Oracle.clientversion()
        

        DATABASE = "HR92U042"
        SCHEMA   = "SYSADM"
        PASSWORD = "sysadm01"
        connstr  = "oracle://{}:{}@{}".format(SCHEMA, PASSWORD, DATABASE)
        engine = sqlalchemy.create_engine(connstr)

        cxora_ini = True

    except sqlalchemy.exc.DatabaseError as e: 
        error_obj, = e.args
        print("Error Code:", error_obj.code)
        print("Error Message:", error_obj.message)
    # else:
    #     print("oracle Version {}".format(ora_version))

    return engine

def get_df_from_db(input_type, my_input):
    
    global engine, cxora_ini
    
    if cxora_ini:
        engine = init_ora()
        if engine:
            cxora_ini = False

    cmp_pnl_df = pd.DataFrame() # Component, Page
    pnl_sub_df = pd.DataFrame() # Page, SubPage
    pnl_rec_df = pd.DataFrame() # Page, Record
    sub_rec_df = pd.DataFrame() # SubPage, Record
    sub_sub_df = pd.DataFrame() # SubPage, SubPage

    if input_type == 'Component':
        cmp_pnl_df = get_pages_in_component(my_input).drop_duplicates() # returns df [pnlgrpname, pnlname] takes Component str
        pnl_sub_df = get_subpages_in_page(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, subpnlname] takes dataframe['pnlname'] 
        pnl_rec_df = get_records_in_pages(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        sub_rec_df = get_records_in_subpages(pnl_sub_df['subpnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['subpnlname']
    if input_type == 'Page':
        cmp_pnl_df = get_component_from_page(my_input).drop_duplicates()
        pnl_sub_df = get_subpages_in_page(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, subpnlname] takes str
        pnl_rec_df = get_records_in_pages(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        sub_rec_df = get_records_in_subpages(pnl_sub_df['subpnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
    if input_type == 'Record':
            # Temporary 
            cmp_pnl_df_1 = pd.DataFrame()
            cmp_pnl_df_2 = pd.DataFrame()
            
            # Retrieve Only pages from the record
            pnl_rec_df = get_pages_from_record(my_input) # returns df [pnlname, recname] takes str
            # Retrieve Only subpages from the record
            sub_rec_df = get_subpages_from_record(my_input)

            # Build Pages/Subpages tree
            # Case 1: Record in Subpage. Find Subpages and Pages related.
            # Case 2: Record in Page. Find Subpages and Pages related.
            if sub_rec_df.size > 0:
                # get Page - SubPage
                pnl_sub_df = get_pages_from_subpages(sub_rec_df['pnlname']).drop_duplicates() # returns df [pnlname, subpnlname] takes
                # get SubPage - SubPage
                sub_sub_df = get_subpages_from_subpages(sub_rec_df['pnlname']).drop_duplicates() # returns df [pnlname, subpnlname] takes
            
                # get related components from the page tree obtained from subpages
                cmp_pnl_df_1 = get_component_from_pages(pnl_sub_df['pnlname']).drop_duplicates() # returns df [pnlgrpname, pnlname] takes str
            
            # get related components from pages obtained from Record
            if pnl_rec_df.size > 0:
                cmp_pnl_df_2 = get_component_from_pages(pnl_rec_df['pnlname']).drop_duplicates() # returns df [pnlgrpname, pnlname] takes str

            # concat both pd.DataFrames of components
            cmp_pnl_df = pd.concat([cmp_pnl_df_1, cmp_pnl_df_2], ignore_index=True)
    
    return cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df

# def simple_query(select, table, clause):    
#     query = f"select distinct {select} from {table} where {clause}"
#     df_all = pd.read_sql(query, con=engine)
#     return df_all

def get_pages_in_component(component_name: str):
    # if read_database:
    query = f"select pnlgrpname, pnlname from pspnlgroup where pnlgrpname = '{component_name}'"
    df_all = pd.read_sql(query, con=engine)
    
    return df_all

# When input type is Page
def get_component_from_page(page_name: str):
    # first query by Component
    query = f"select pnlgrpname, pnlname from pspnlgroup where pnlname = '{page_name}'"
    df_all = pd.read_sql(query, con=engine)
    
    return df_all


# Get all SubPages in Page > Page::Page edges
# Returns dataframe with 2 columns, "Page" and "SubPage"
def get_subpages_in_page(page_name_df): 
    if isinstance(page_name_df,pd.DataFrame):
        in_stmt = '\', \''.join(page_name_df['pnlname'].to_list())
    elif isinstance(page_name_df,pd.Series):
        in_stmt = '\', \''.join(page_name_df.to_list())
    query = f"select distinct pnlname, subpnlname from pspnlfield where subpnlname > ' ' start with pnlname in ('{in_stmt}') connect by nocycle prior subpnlname = pnlname"
    df_all = pd.read_sql(query, con=engine)
   
    return df_all



# Get Records in a Page > Page::Record::Field edges
# Returns dataframe with 3 columns, "Page", "Record" and "Field"

def get_records_in_pages(pages_df):
    if isinstance(pages_df,pd.DataFrame):
        in_stmt = '\', \''.join(pages_df['pnlname'].to_list())
    elif isinstance(pages_df,pd.Series):
        in_stmt = '\', \''.join(pages_df.to_list())
    query = f"select distinct pnlname, recname from pspnlfield where pnlname in ('{in_stmt}') and fieldname > ' '"
    df_all = pd.read_sql(query, con=engine)
    return df_all

# Get only Pages for one Record
def get_pages_from_record(recname: str):
    # query = f"select distinct a.pnlname, a.recname from pspnlfield a where a.recname = '{recname}' and a.fieldname = ' ' and not exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname)"
    query = f"select distinct a.pnlname, a.recname from pspnlfield a where a.recname = '{recname}' and not exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname)"
    page_name = pd.read_sql(query, con=engine)

    return page_name

# Get only SubPages for one Record
def get_subpages_from_record(recname: str):
    # query = f"select distinct a.pnlname, a.recname from pspnlfield a where a.recname = '{recname}' and a.fieldname = ' ' and exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname)"
    query = f"select distinct a.pnlname, a.recname from pspnlfield a where a.recname = '{recname}' and exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname)"
    page_name = pd.read_sql(query, con=engine)

    return page_name



# Get a Page Tree from a pd.Series of Pages/Subpages
def get_pages_from_subpages(subpage_name_df): 
    if isinstance(subpage_name_df,pd.Series):
        in_stmt = '\', \''.join(subpage_name_df.to_list())
    query = f"""select distinct a.pnlname, a.subpnlname 
                from pspnlfield a 
                where a.subpnlname > ' ' and not exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname) 
                start with a.subpnlname in ('{in_stmt}') 
                connect by prior a.pnlname = a.subpnlname
        """
    df_all = pd.read_sql(query, con=engine)
    print(f"get_pages_from_subpages:\n{df_all}")
    return df_all



# Get a SubPage Tree from a pd.Series of Pages/Subpages
def get_subpages_from_subpages(subpage_name_df): 
    if isinstance(subpage_name_df,pd.Series):
        in_stmt = '\', \''.join(subpage_name_df.to_list())
    query = f"""select distinct a.pnlname, a.subpnlname 
                from pspnlfield a 
                where a.subpnlname > ' ' and exists (select distinct 'Y' from pspnlfield b where a.pnlname = b.subpnlname) 
                start with a.subpnlname in ('{in_stmt}') 
                connect by prior a.pnlname = a.subpnlname
        """
    df_all = pd.read_sql(query, con=engine)
    print(f"get_subpages_from_subpages:\n{df_all}")
    return df_all    

# Get Components from a pd.Series of Pages
def get_component_from_pages(pagename_df):
    if isinstance(pagename_df,pd.Series):
        in_stmt = '\', \''.join(pagename_df.to_list())
    query = f"select pnlgrpname, pnlname from pspnlgroup where pnlname in ('{in_stmt}')"
    df_all = pd.read_sql(query, con=engine)
    
    return df_all

# Get Records in a SubPage > SubPage::Record::Field edges
def get_records_in_subpages(pages_df):
    if isinstance(pages_df,pd.DataFrame):
        in_stmt = '\', \''.join(pages_df['subpnlname'].to_list())
    elif isinstance(pages_df,pd.Series):
        in_stmt = '\', \''.join(pages_df.to_list())
    query = f"select distinct pnlname, recname from pspnlfield where pnlname in ('{in_stmt}') and fieldname > ' '"
    df_all = pd.read_sql(query, con=engine)
    return df_all

# Returns pd.DataFrame with 2 columns: Node and "Path to Event"
# obj_name is a Dictionary of the Node key (name) and value (label)
def query_peoplecode_events(qry_name, obj_name):
    
    in_ = []
    for i in obj_name:
        in_.append(i['label'])
    
    in_stmt = '\', \''.join(in_)
    and_clause = f" and objectvalue1 in ('{in_stmt}')"

    # Component Events
    if qry_name == "qry_event_component": # Component.event (PreBuild, PostBuild, SavePostChange, SavePreChange) >> OBJECTID1 = 10, OBJECTID3 = 12 (Event)
        qry_name = "SELECT OBJECTVALUE1 as node, (OBJECTVALUE1 || '.' || OBJECTVALUE2 || '.' || OBJECTVALUE3) as eventpath FROM PSPCMPROG WHERE OBJECTID1 = 10 AND OBJECTID3 = 12" 
    elif qry_name == "qry_event_comp_rec": # Component.Record.event (RowInit, RowInsert, RowDelete, RowSelect, SaveEdit, SavePostChange, SavePreChange)  >> OBJECTID1 = 10, OBJECTID3 = 1 (Record) AND OBJECT4 = 12 (Event)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || '.' || OBJECTVALUE2 || '.' || OBJECTVALUE3 || '.' || OBJECTVALUE4 as eventpath FROM PSPCMPROG WHERE OBJECTID1 = 10 AND OBJECTID3 = 1 AND OBJECTID4 = 12"
    elif qry_name == "qry_event_comp_rec_field": # Component.Record.Field.event (FieldChange, FieldDefault, FieldEdit, PrePopUp)  >> OBJECTID1 = 10, OBJECTID3 = 1 (Record), OBJECT4 = 2 (Field) AND OBJECT5 = 12 (Event)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || '.' || OBJECTVALUE2 || '.' || OBJECTVALUE3 || '.' || OBJECTVALUE4 || '.' || OBJECTVALUE5 as eventpath FROM PSPCMPROG WHERE OBJECTID1 = 10 AND OBJECTID3 = 1 AND OBJECTID4 = 2 AND OBJECTID5 = 12"
    # Page Events
    elif qry_name == "qry_event_page": # Page.event (Activate) >> OBJECTID1 = 9, OBJECTID2 = 12
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || '.' || OBJECTVALUE2 as eventpath FROM PSPCMPROG WHERE OBJECTID1 = 9 AND OBJECTID2 = 12"
    # Record Events
    elif qry_name == "qry_event_record_field": # Record.event (All... FieldFormula, FieldChange, FieldDefault, FieldEdit, PrePopUp, RowInit, RowInsert, RowDelete, RowSelect, SaveEdit, SavePostChange, SavePreChange, Workflow, SearchSave, SearchInit) >> OBJECTID1 = 1, OBJECTID2 = 2 (Field), OBJECT3 = 12 (Event)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || '.' || OBJECTVALUE2 || '.' || OBJECTVALUE3 as eventpath  FROM PSPCMPROG WHERE OBJECTID1 = 1 AND OBJECTID2 = 2 AND OBJECTID3 = 12"
    # Application Package Classes
    elif qry_name == "qry_appPckg_l1": # ApplicationPackage.Class.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 107 (class), OBJECTID3 = 12 (OnExecute)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || ':' || OBJECTVALUE2 || ':' || OBJECTVALUE3 as eventpath  FROM PSPCMPROG WHERE OBJECTID1 = 104 AND OBJECTID2 = 107 AND OBJECTID3 = 12"
    elif qry_name == "qry_appPckg_l2": # ApplicationPackage.SubAppPckg.Class.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 105 (SubPackage), OBJECTID3 107 (class), OBJECTID4 = 12 (OnExecute)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || ':' || OBJECTVALUE2 || ':' || OBJECTVALUE3 || ':' || OBJECTVALUE4 as eventpath  FROM PSPCMPROG WHERE OBJECTID1 = 104 AND OBJECTID2 = 105 AND OBJECTID3 = 107 AND OBJECTID4 = 12"
    elif qry_name == "qry_appPckg_l3": # ApplicationPackage.SubAppPckg.SubAppPckg.event (onExecute) >> OBJECTID1 = 104 (Package), OBJECTID2 = 105 (SubPackage), OBJECTID3 = 106 (SubPackage), OBJECTID4 107 (class), OBJECTID5 = 12 (OnExecute)
        qry_name = "SELECT OBJECTVALUE1 as node, OBJECTVALUE1 || ':' || OBJECTVALUE2 || ':' || OBJECTVALUE3 || ':' || OBJECTVALUE4 || ':' || OBJECTVALUE5 as eventpath  FROM PSPCMPROG WHERE OBJECTID1 = 104 AND OBJECTID2 = 105 AND OBJECTID3 = 106 AND OBJECTID4 = 107 AND OBJECTID5 = 12"
    else:
        exit(0)
    
    # optional AND Clause
    if and_clause:
        qry_name = qry_name + and_clause 

    # Query the Database
    df_all = pd.read_sql(qry_name, con=engine)
    return df_all