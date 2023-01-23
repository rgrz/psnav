import graphviz
import pandas as pd
import os

import from_db
import from_file

global file_digest, file_digest_flag

# When input type is Component
# Returns dataframe with one column, "Page"


# get PeopleCode for specific object
# Object Type: 10 (Component), 9 (Page) or 1 (Record)
# def get_events(obj_type, obj_name):
#     print(f'get events for {obj_type} {obj_name}')
#     df_all = get_events_bulk(obj_type, obj_name)
   
#     return df_all

# From App Designer report we can extract event calls to Functions and imports from App Packages
# filter Declare Function *
# Record.event (FieldFormula,... but it can be located in different events actually) >> OBJECTID1 = 1, OBJECTID2 = 2 (Field), OBJECT3 = 12 (Event)
# Identify Node containing the event by Path
file_digest = []
file_digest_flag = {}

def lines_from_file(path):
    with open(path) as handle:
        for line in handle:
            yield line

def matching_lines(lines, pattern):
    for line in lines:
        if pattern in line:
            yield line

# def parse_appdesigner_report(path, filename, extension):
#     global file_digest, file_digest_flag
#     p = os.path.join('.', path, ''.join(filename+ '.' + extension))

#     obj_list = []
#     pkg_list = []
#     fcall_list = []
#     func_list = []
    
#     lines = lines_from_file(p)

#     matching = matching_lines(lines, '[')


def parse_appdesigner_report(path, filename, extension):
    global file_digest, file_digest_flag
    p = os.path.join('.', path, ''.join(filename+ '.' + extension))
    print(f'filename: {filename}')

    if os.path.isfile(p):
        # parse P42_pcode_utf8.txt!
        obj_list = []
        pkg_list = []
        fcall_list = []
        func_list = []
        try:
            with open(p, 'r') as file_p42:
                event_idx = 0
                print(f"file opened!")
                # for line in file_p42:
                    # for line in lines_from_file(p):
                for line in file_p42:
                    # print(f"reading line {line}")
                    if line.startswith('['):
                        event_idx += 1 # event_idx idetifies each event with a unique number.
                        obj_list.append([str(event_idx)]+line[1:len(line)-2].split('.'))
                    # parse Application Packages Calls "^import "
                    if line.startswith('import '):
                        pkg_list.append([str(event_idx)]+line[7:line.find(';')].split(':'))
                    # parse FieldFormula calls "^Declare Function "
                    if line.startswith('Declare Function '):
                        fcall_list.append([str(event_idx)]+line[17:line.find(';')].split(' '))
                    # parse Function definition "^Function "
                    if line.startswith('Function '):
                        func_list.append([str(event_idx), line[9:line.find('(')]])
        except (IOError, OSError) as e:            
            print(f"Error opening {filename}: {e}")

        print(f"Read Objects::")    
        print(f"# objects in obj_list read: {len(obj_list)}")
        idx = 0
        new_obj_list = []
        # clean obj_list in case there are entries for application engine (8 keys). Only accept 6 keys max
        for obj in obj_list:
            if len(obj) <= 6:
                new_obj_list.append(obj)
        # print(f"size of obj_list read: {obj_list.size}")
        
        obj_df = pd.DataFrame(new_obj_list, columns=['id', 'key1', 'key2', 'key3', 'key4', 'key5']).fillna(' ')
        print(f"# objects in pkg_list: {len(pkg_list)}")

        pkg_imp_df = pd.DataFrame(pkg_list, columns=['id', 'key1', 'key2', 'key3', 'key4']).fillna(' ')
        print(f"# objects in fcall_list: {len(fcall_list)}")
        func_call_df = pd.DataFrame(fcall_list, columns=['id', 'key1', 'key2', 'key3', 'key4']).fillna(' ')
        print(f"# objects func_list: {len(func_list)}")
        func_defn_df = pd.DataFrame(func_list, columns=['id', 'key1']).fillna(' ')
        file_digest.append([filename, {'objects': obj_df, 'packages': pkg_imp_df, 'functions': func_call_df, 'funcdefn': func_defn_df}])
        file_digest_flag[filename] = True

    else:
        print('path does not exists')

def parse_pcode(custom_obj, event_type, events_df):
    global file_digest, file_digest_flag
    
    print(f"events_df:\n {events_df.size}\n{events_df}")
    if custom_obj == 'Custom': 
        filename = 'IFAUAT_pcode_utf8'
    else:
        filename = 'P42_pcode_utf8'

    if filename not in file_digest_flag.keys():
            parse_appdesigner_report('data', filename, 'txt')
            
    obj_df = pd.DataFrame()
    pkg_imp_df = pd.DataFrame()
    func_call_df = pd.DataFrame()
    func_defn_df = pd.DataFrame()

    if len(file_digest) >= 1:
        for element in file_digest:
            if element[0] == filename:
                obj_df = element[1]['objects'].copy()
                pkg_imp_df = element[1]['packages'].copy()
                func_call_df = element[1]['functions'].copy()
                func_defn_df = element[1]['funcdefn'].copy()
    else:
        exit(1)

    # Note: eventpath column was renamed to Edge in Component PeopleCode
    new_event_df = events_df.eventpath.str.split('.', expand=True).copy()

    new_columns = []
    for col in new_event_df.columns:
        col = col + 1
        new_columns.append(f'key{col}')

    new_event_df.columns = new_columns
    print(f'Columns for {event_type} \n{events_df}\n are {new_columns}')
    # Note: it will create a new Dataframe
    selected_obj_df = pd.merge(obj_df, new_event_df, how='inner', left_on=new_columns, right_on=new_columns)
    
    if event_type == 'Application Packages':
        selected_df = pkg_imp_df[pkg_imp_df['id'].isin(selected_obj_df['id'])]
    if event_type == 'Function Calls':
        selected_df = func_call_df[func_call_df['id'].isin(selected_obj_df['id'])]
    if event_type == 'Function Definitions':
        selected_df = func_defn_df[func_defn_df['id'].isin(selected_obj_df['id'])]
    
    print(f'---------------------------pcode_drilldown: {event_type} \n{selected_df} \n-------------------')

    merged_df = pd.merge(obj_df, selected_df, how='inner', on='id')

    selected_events_df = pd.DataFrame()

    if len(new_columns) == 2:
        selected_events_df['Node'] = merged_df['key1_x'] + '.' + merged_df['key2_x']
        selected_events_df['Edge'] = merged_df['key1_y'] + '.' + merged_df['key2_y']
    elif len(new_columns) == 3:
        selected_events_df['Node'] = merged_df['key1_x'] + '.' + merged_df['key2_x'] + '.' + merged_df['key3_x']
        selected_events_df['Edge'] = merged_df['key1_y'] + '.' + merged_df['key2_y'] + '.' + merged_df['key3_y']
    elif len(new_columns) == 4:
        selected_events_df['Node'] = merged_df['key1_x'] + '.' + merged_df['key2_x'] + '.' + merged_df['key3_x'] + '.' + merged_df['key4_x']
        selected_events_df['Edge'] = merged_df['key1_y'] + '.' + merged_df['key2_y'] + '.' + merged_df['key3_y'] + '.' + merged_df['key4_y']
    elif len(new_columns) == 5:
        selected_events_df['Node'] = merged_df['key1_x'] + '.' + merged_df['key2_x'] + '.' + merged_df['key3_x'] + '.' + merged_df['key4_x'] + '.' + merged_df['key5_x']
        selected_events_df['Edge'] = merged_df['key1_y'] + '.' + merged_df['key2_y'] + '.' + merged_df['key3_y'] + '.' + merged_df['key4_y'] + '.' + merged_df['key5_y']

    return selected_events_df

# query for specific objects that are selected in the graph
def query_custom_objects():
    projectitem_df = pd.read_csv('./reports/psprojectitem.csv', sep=',')
    ifa_projects = projectitem_df[projectitem_df['PROJECTNAME'].str.contains('IFA_', regex=True)].copy()

    # objecttype:
    # 0 Record
    # 5 Page
    # 7 Component
    # 8 Record PeopleCode
    # 44 Page PeopleCode
    # 46 Component PeopleCode
    # 47 Component Record PeopleCode
    # 48 Component Record Field PeopleCode
    # 57 Application Packages
    # 58 Application Package PeopleCode

def spaghetti_df(params):

    input_type = params[0]
    my_input = params[1]
    detail_level = params[2]
    pcode_detail = params[3]
    custom_obj = params[4]
    
    list_of_nodes = {}
    list_of_edges = []

    component_nodes = []
    page_nodes = []
    subpage_nodes = []
    record_nodes = []
    record_nodes_sub = []

    cmp_pnl_edges = []
    pnl_sub_edges = []
    pnl_rec_edges = []
    sub_rec_edges = []

    activate_nodes = []
    activate_edges = []
    cmp_pcode_nodes = []
    cmp_pcode_edges = []
    cmp_rec_pcode_nodes = []
    cmp_rec_pcode_edges = []
    cmp_recfld_pcode_nodes = []
    cmp_recfld_pcode_edges = []

    app_pkg_cmp_imports_nodes = []
    app_pkg_cmp_imports_edges = []
    app_pkg_cmp_rec_imports_nodes = []
    app_pkg_cmp_rec_imports_edges = []
    app_pkg_cmp_recf_imports_nodes = []
    app_pkg_cmp_recf_imports_edges = []
    app_pkg_page_imports_nodes = []
    app_pkg_page_imports_edges = []

    function_calls_cmp_imports_nodes = []
    function_calls_cmp_imports_edges = []
    function_calls_cmp_rec_imports_nodes = []
    function_calls_cmp_rec_imports_edges = []
    function_calls_cmp_recf_imports_nodes = []
    function_calls_cmp_recf_imports_edges = []
    function_calls_page_imports_nodes = []
    function_calls_page_imports_edges = []
    function_defn_page_imports_nodes = []
    function_defn_page_imports_edges = []

    # cmp_pnl_df = pd.DataFrame()
    cmp_events_df = pd.DataFrame()

    if custom_obj == 'Custom':
        cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df = from_file.get_df_from_file(input_type, my_input)
    else:
        cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df = from_db.get_df_from_db(input_type, my_input)

    cmp_pnl_df.sort_values(by=['pnlgrpname', 'pnlname'], inplace=True)
    pnl_sub_df.sort_values(by=['pnlname', 'subpnlname'], inplace=True)
    pnl_rec_df.sort_values(by=['pnlname', 'recname'], inplace=True)
    sub_rec_df.sort_values(by=['pnlname', 'recname'], inplace=True)

    if input_type == 'Component':

        
        # cmp_pnl_df = get_pages_in_component(my_input).drop_duplicates() # returns df [pnlgrpname, pnlname] takes Component str
        # pnl_sub_df = get_subpages_in_page(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, subpnlname] takes dataframe['pnlname'] 
        # pnl_rec_df = get_records_in_pages(cmp_pnl_df['pnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['pnlname']
        # sub_rec_df = get_records_in_subpages(pnl_sub_df['subpnlname']).drop_duplicates() # returns df [pnlname, recname] takes dataframe['subpnlname']
        
        component_nodes.append({'name':''.join(['component_',my_input]), 'label':my_input, 'attr':''})
        list_of_nodes['Component'] = component_nodes

        if 'Page' in detail_level:
            for i, row in cmp_pnl_df.iterrows():
                page_nodes.append({'name': ''.join(['page_',row['pnlname']]), 'label':row['pnlname'], 'attr':''})
                cmp_pnl_edges.append([''.join(['component_',row['pnlgrpname']]), ''.join(['page_',row['pnlname']])])
            list_of_nodes['Page'] = page_nodes
            list_of_edges.append(cmp_pnl_edges)
        if 'SubPages' in detail_level:
            for i, row in pnl_sub_df.iterrows():
                subpage_nodes.append({'name':''.join(['page_',row['subpnlname']]), 'label':row['subpnlname'], 'attr':''})
                pnl_sub_edges.append([''.join(['page_',row['pnlname']]), ''.join(['page_',row['subpnlname']])])
            list_of_nodes['SubPage'] = subpage_nodes
            list_of_edges.append(pnl_sub_edges)
        if 'Record' in detail_level:
            if 'Page' in detail_level:
                for i, row in pnl_rec_df.iterrows():
                    record_nodes.append({'name':''.join(['recname_',row['recname']]), 'label':row['recname'], 'attr':''})
                    pnl_rec_edges.append([''.join(['page_',row['pnlname']]), ''.join(['recname_',row['recname']])])
                list_of_nodes['Record'] = record_nodes
                list_of_edges.append(pnl_rec_edges)
            if 'SubPage' in detail_level:
                for i, row in sub_rec_df.iterrows():
                    record_nodes_sub.append({'name':''.join(['recname_',row['recname']]), 'label':row['recname'], 'attr':''})
                    sub_rec_edges.append([''.join(['page_',row['pnlname']]), ''.join(['recname_',row['recname']])])
                list_of_nodes['Record'] = record_nodes_sub
                list_of_edges.append(sub_rec_edges)

    if input_type == 'Page':
        # cmp_pnl_df = get_component_from_page(my_input)
        # pnl_sub_df = get_subpages_in_page(cmp_pnl_df['pnlname']) # returns df [pnlname, subpnlname] takes str
        # pnl_rec_df = get_records_in_pages(cmp_pnl_df['pnlname']) # returns df [pnlname, recname] takes dataframe['pnlname']
        # sub_rec_df = get_records_in_subpages(pnl_sub_df['subpnlname']) # returns df [pnlname, recname] takes dataframe['pnlname']
        # cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df = from_db.get_df_from_db(input_type, my_input)

        page_nodes.append({'name': ''.join(['page_',my_input]), 'label':my_input})
        list_of_nodes['Page'] = page_nodes

        if 'Component' in detail_level:
            for i, row in cmp_pnl_df.iterrows():
                component_nodes.append({'name':''.join(['component_',row['pnlgrpname']]), 'label':row['pnlgrpname'], 'attr':''})
                cmp_pnl_edges.append([''.join(['page_',row['pnlname']]), ''.join(['component_',row['pnlgrpname']])])
            list_of_nodes['Component'] = component_nodes
            list_of_edges.append(cmp_pnl_edges)
        if 'SubPages' in detail_level:
            for i, row in pnl_sub_df.iterrows():
                subpage_nodes.append({'name':''.join(['page_',row['subpnlname']]), 'label':row['subpnlname'], 'attr':''})
                pnl_sub_edges.append([''.join(['page_',row['pnlname']]), ''.join(['page_',row['subpnlname']])])
            list_of_nodes['SubPage'] = subpage_nodes
            list_of_edges.append(pnl_sub_edges)
        if 'Record' in detail_level:
            if 'Page' in detail_level:
                for i, row in pnl_rec_df.iterrows():
                    record_nodes.append({'name':''.join(['recname_',row['recname']]), 'label':row['recname'], 'attr':''})
                    pnl_rec_edges.append([''.join(['page_',row['pnlname']]), ''.join(['recname_',row['recname']])])
                list_of_nodes['Record'] = record_nodes
                list_of_edges.append(pnl_rec_edges)
            if 'SubPages' in detail_level:
                for i, row in sub_rec_df.iterrows():
                    record_nodes_sub.append({'name':''.join(['recname_',row['recname']]), 'label':row['recname'], 'attr':''})
                    sub_rec_edges.append([''.join(['page_',row['pnlname']]), ''.join(['recname_',row['recname']])])
                list_of_nodes['Record'] = list_of_nodes['Record'] + record_nodes_sub
                list_of_edges.append(sub_rec_edges)
    if input_type == 'Record':
        
        # cmp_pnl_df, pnl_sub_df, pnl_rec_df, sub_rec_df, sub_sub_df = from_db.get_df_from_db(input_type, my_input)
        
        record_nodes.append({'name':''.join(['recname_',my_input]), 'label':my_input, 'attr':''})
        list_of_nodes['Record'] = record_nodes

        subpage_nodes_ind = []
        pnl_sub_edges_ind = []
        page_nodes_ind = []
        pnl_rec_edges_ind = []

        # Direct Pages where the Record is present
        if 'Page' in detail_level:
            for i, row in pnl_rec_df.iterrows():
                page_nodes.append({'name': ''.join(['page_',row['pnlname']]), 'label':row['pnlname'], 'attr':''})     
                pnl_rec_edges.append([''.join(['recname_',row['recname']]), ''.join(['page_',row['pnlname']])])
            list_of_nodes['Page'] = page_nodes 
            list_of_edges.append(pnl_rec_edges)
        # Direct SubPages where the Record is present
        if 'SubPages' in detail_level:
            # sub_rec_df = pnl_rec_df[pnl_rec_df['PNLNAME'].isin(pnl_rec_df['SUBPNLNAME'])].copy()
            for i, row in sub_rec_df.iterrows():
                subpage_nodes.append({'name':''.join(['page_',row['pnlname']]), 'label':row['pnlname'], 'attr':''})
                sub_rec_edges.append([''.join(['recname_',row['recname']]), ''.join(['page_',row['pnlname']])])
            list_of_nodes['SubPage'] = subpage_nodes
            list_of_edges.append(sub_rec_edges)
        # Page-SubPages Tree where the Record is Present within a SubPage
        if 'Page' in detail_level:
            for i, row in pnl_sub_df.iterrows():
                page_nodes_ind.append({'name': ''.join(['page_',row['pnlname']]), 'label':row['pnlname'], 'attr':''})   
                pnl_rec_edges_ind.append([''.join(['page_',row['subpnlname']]), ''.join(['page_',row['pnlname']])])
            list_of_nodes['Page'] = list_of_nodes['Page'] + page_nodes_ind
            list_of_edges.append(pnl_rec_edges_ind)
        # SubPage-SubPages Tree where the Record is Present within a SubPage
        if 'SubPages' in detail_level:
            for i, row in sub_sub_df.iterrows():
                subpage_nodes_ind.append({'name': ''.join(['page_',row['pnlname']]), 'label':row['pnlname'], 'attr':''})   
                pnl_sub_edges_ind.append([''.join(['page_',row['subpnlname']]), ''.join(['page_',row['pnlname']])])
            list_of_nodes['SubPage'] = list_of_nodes['SubPage'] + subpage_nodes_ind
            list_of_edges.append(pnl_sub_edges_ind)
        # Components where the Record is present directly and indirectly 
        if 'Component' in detail_level:
            for i, row in cmp_pnl_df.iterrows():
                component_nodes.append({'name':''.join(['component_',row['pnlgrpname']]), 'label':row['pnlgrpname'], 'attr':''})
                cmp_pnl_edges.append([''.join(['page_',row['pnlname']]), ''.join(['component_',row['pnlgrpname']])])
            list_of_nodes['Component'] = component_nodes
            list_of_edges.append(cmp_pnl_edges)      

    if 'Page' in detail_level:
        if 'Page PeopleCode' in pcode_detail:
            if custom_obj == 'Custom':
                pnl_events_df = from_file.fquery_peoplecode_events('qry_event_page', list_of_nodes['Page'])
            else:
                pnl_events_df = from_db.query_peoplecode_events('qry_event_page', list_of_nodes['Page'])
            if pnl_events_df.size > 0:
                for i, row in pnl_events_df.iterrows():
                    activate_nodes.append({'name':row['eventpath'], 'label':row['eventpath'], 'attr':''})
                    activate_edges.append([''.join(['page_',row['node']]), row['eventpath']])
                list_of_nodes['Activate Events'] = activate_nodes
                list_of_edges.append(activate_edges) 
    if 'Component' in detail_level:
        if 'Component PeopleCode' in pcode_detail:
            if custom_obj == 'Custom':
                cmp_events_df = from_file.fquery_peoplecode_events('qry_event_component', list_of_nodes['Component']) # component_nodes a list of dictionaries {name, label} for each node
            else:    
                cmp_events_df = from_db.query_peoplecode_events('qry_event_component', list_of_nodes['Component']) # component_nodes a list of dictionaries {name, label} for each node
            if cmp_events_df.size > 0:
                for i, row in cmp_events_df.iterrows():
                    cmp_pcode_nodes.append({'name':row['eventpath'], 'label':row['eventpath'], 'attr':''})
                    cmp_pcode_edges.append([''.join(['component_',row['node']]), row['eventpath']])
                list_of_nodes['Component PeopleCode'] = cmp_pcode_nodes
                list_of_edges.append(cmp_pcode_edges) 
        if 'Component Record PeopleCode' in pcode_detail:
            if custom_obj == 'Custom':
                cmp_rec_events_df = from_file.fquery_peoplecode_events('qry_event_comp_rec', list_of_nodes['Component'])
            else:
                cmp_rec_events_df = from_db.query_peoplecode_events('qry_event_comp_rec', list_of_nodes['Component'])
            if cmp_rec_events_df.size > 0:
                for i, row in cmp_rec_events_df.iterrows():
                    cmp_rec_pcode_nodes.append({'name':row['eventpath'], 'label':row['eventpath'], 'attr':''})
                    cmp_rec_pcode_edges.append([''.join(['component_',row['node']]), row['eventpath']])
                list_of_nodes['Component Record PeopleCode'] = cmp_rec_pcode_nodes
                list_of_edges.append(cmp_rec_pcode_edges) 
        if 'Component RecField PeopleCode' in pcode_detail:
            cmp_rec_fld_events_df = from_db.query_peoplecode_events('qry_event_comp_rec_field', list_of_nodes['Component'])
            if cmp_rec_fld_events_df.size > 0:
                for i, row in cmp_rec_fld_events_df.iterrows():
                    cmp_recfld_pcode_nodes.append({'name':row['eventpath'], 'label':row['eventpath'], 'attr':''})
                    cmp_recfld_pcode_edges.append([''.join(['component_',row['node']]), row['eventpath']])
                list_of_nodes['Component RecField PeopleCode'] = cmp_recfld_pcode_nodes
                list_of_edges.append(cmp_recfld_pcode_edges) 
    if 'Record' in detail_level:
        print(f"TODO: Get qry_event_record_field")

    if 'Application Packages' in pcode_detail:
        list_of_nodes['Application Packages'] = []
        
        if 'Component' in detail_level:
            if 'Component PeopleCode' in pcode_detail:
                # print(f"cmp_events_df {cmp_events_df}")
                app_pkg_cmp_imports_df = pd.DataFrame()
                app_pkg_cmp_imports_df = parse_pcode(custom_obj, 'Application Packages', cmp_events_df)
                if app_pkg_cmp_imports_df.size > 0:
                    print(f"Application Packages in Component PeopleCode: {app_pkg_cmp_imports_df.size}")
                    for i, row in app_pkg_cmp_imports_df.iterrows():
                        # print(f'Component PeopleCode Row: {row}')
                        app_pkg_cmp_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        # app_pkg_cmp_imports_edges.append([''.join(['component_',row['Node']]), row['Edge']])
                        app_pkg_cmp_imports_edges.append([row['Node'], row['Edge']])
                    list_of_nodes['Application Packages'] = app_pkg_cmp_imports_nodes
                    list_of_edges.append(app_pkg_cmp_imports_edges) 
            if 'Component Record PeopleCode' in pcode_detail:
                app_pkg_cmp_rec_imports_df = pd.DataFrame()
                app_pkg_cmp_rec_imports_df = parse_pcode(custom_obj, 'Application Packages', cmp_rec_events_df)
                if app_pkg_cmp_rec_imports_df.size > 0:
                    for i, row in app_pkg_cmp_rec_imports_df.iterrows():
                        app_pkg_cmp_rec_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        app_pkg_cmp_rec_imports_edges.append([row['Node'], row['Edge']])
                    list_of_nodes['Application Packages'] = list_of_nodes['Application Packages'] + app_pkg_cmp_rec_imports_nodes
                    list_of_edges.append(app_pkg_cmp_rec_imports_edges) 
            if 'Component RecField PeopleCode' in pcode_detail:
                app_pkg_cmp_recf_imports_df = pd.DataFrame()
                app_pkg_cmp_recf_imports_df = parse_pcode(custom_obj, 'Application Packages', cmp_rec_fld_events_df)
                if app_pkg_cmp_recf_imports_df.size > 0:
                    for i, row in app_pkg_cmp_recf_imports_df.iterrows():
                        app_pkg_cmp_recf_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        app_pkg_cmp_recf_imports_edges.append([row['Node'], row['Edge']])
                    list_of_nodes['Application Packages'] = list_of_nodes['Application Packages'] + app_pkg_cmp_recf_imports_nodes
                    list_of_edges.append(app_pkg_cmp_recf_imports_edges) 
        if 'Page' in detail_level:
            if 'Page PeopleCode' in pcode_detail:
                app_pkg_page_imports_df = pd.DataFrame()
                print(f'pnl_events_df {pnl_events_df}')
                app_pkg_page_imports_df = parse_pcode(custom_obj, 'Application Packages', pnl_events_df)
                print(f"Application Packages in Page PeopleCode: {app_pkg_page_imports_df.size}")

                if app_pkg_page_imports_df.size > 0:
                    for i, row in app_pkg_page_imports_df.iterrows():
                        app_pkg_page_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        app_pkg_page_imports_edges.append([row['Node'], row['Edge']])
                    list_of_nodes['Application Packages'] = list_of_nodes['Application Packages'] + app_pkg_page_imports_nodes
                    list_of_edges.append(app_pkg_page_imports_edges) 
        
    if 'Function Calls' in pcode_detail:
        list_of_nodes['Function Calls'] = []
        
        if 'Component PeopleCode' in pcode_detail:
            function_calls_cmp_imports_df = pd.DataFrame()
            function_calls_cmp_imports_df = parse_pcode(custom_obj, 'Function Calls', cmp_events_df)
            if function_calls_cmp_imports_df.size > 0:
                    for i, row in function_calls_cmp_imports_df.iterrows():
                        function_calls_cmp_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        function_calls_cmp_imports_edges.append([row['Node'], row['Edge']])
                    list_of_nodes['Function Calls'] = function_calls_cmp_imports_nodes
                    list_of_edges.append(function_calls_cmp_imports_edges) 
        if 'Component Record PeopleCode' in pcode_detail:
            function_calls_cmp_rec_imports_df = pd.DataFrame()
            function_calls_cmp_rec_imports_df = parse_pcode(custom_obj, 'Function Calls', cmp_rec_events_df)
            if function_calls_cmp_rec_imports_df.size > 0:
                    for i, row in function_calls_cmp_rec_imports_df.iterrows():
                        function_calls_cmp_rec_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        function_calls_cmp_rec_imports_edges.append([row['Node'], row['Edge']])            
                    list_of_nodes['Function Calls'] = list_of_nodes['Function Calls'] + function_calls_cmp_rec_imports_nodes
                    list_of_edges.append(function_calls_cmp_rec_imports_edges)                     
        if 'Component RecField PeopleCode' in pcode_detail:
            function_calls_cmp_recf_imports_df = pd.DataFrame()
            function_calls_cmp_recf_imports_df = parse_pcode(custom_obj, 'Function Calls', cmp_rec_fld_events_df)
            if function_calls_cmp_recf_imports_df.size > 0:
                    for i, row in function_calls_cmp_recf_imports_df.iterrows():
                        function_calls_cmp_recf_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        function_calls_cmp_recf_imports_edges.append([row['Node'], row['Edge']])  
                    list_of_nodes['Function Calls'] = list_of_nodes['Function Calls'] + function_calls_cmp_recf_imports_nodes
                    list_of_edges.append(function_calls_cmp_recf_imports_edges) 
        if 'Page PeopleCode' in pcode_detail:
            function_calls_page_imports_df = pd.DataFrame()
            function_calls_page_imports_df = parse_pcode(custom_obj, 'Function Calls', pnl_events_df)
            if function_calls_page_imports_df.size > 0:
                    for i, row in function_calls_page_imports_df.iterrows():
                        function_calls_page_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                        function_calls_page_imports_edges.append([row['Node'], row['Edge']]) 
                    list_of_nodes['Function Calls'] = list_of_nodes['Function Calls'] + function_calls_page_imports_nodes
                    list_of_edges.append(function_calls_page_imports_edges) 
    if 'Function Definitions' in pcode_detail:
        list_of_nodes['Function Definitions'] = []
        function_defn_page_imports_df = pd.DataFrame()
        function_defn_page_imports_df = parse_pcode(custom_obj, 'Function Definitions', pnl_events_df)
        if function_defn_page_imports_df.size > 0:
                for i, row in function_defn_page_imports_df.iterrows():
                    function_defn_page_imports_nodes.append({'name':row['Edge'], 'label':row['Edge'], 'attr':''})
                    function_defn_page_imports_edges.append([row['Node'], row['Edge']]) 
                list_of_nodes['Function Definitions'] = list_of_nodes['Function Definitions'] + function_defn_page_imports_nodes
                list_of_edges.append(function_defn_page_imports_edges) 
    # for cluster in list_of_nodes:
    #     print(f"Find Customizations in cluster '{cluster}'")
    #     # Retrieve value for current key (cluster)
    #     node_list = list_of_nodes[cluster]
        
    #     if len(node_list) > 0:
    #         print(f"Cluster '{cluster}' node_list has {len(node_list)} elements")
    #         # create a subgraph for current cluster
    #         # Iterate over list of Dictionaries (name, label)
    #         for node in node_list:
    #             print(f'    Node: {node}')
    #             if is_object_customized(cluster, node['label']):
    #                 node['attr'] = {'color':'red'}
    #                 # print(f'*** in cluster {cluster} ***** Object {node["label"]} Customized')

    return [list_of_nodes, list_of_edges]

# global dataframe with customizations
projectitem_df = pd.DataFrame()

def import_customizations():
    global projectitem_df
    projectitem_df = pd.read_csv('./reports/psprojectitem.csv', sep=',')
    
    ifa_projects = projectitem_df[projectitem_df['PROJECTNAME'].str.contains('IFA_', regex=True)].copy()
    print(f'Size of custom dataframe {ifa_projects.size}')

    return ifa_projects

def is_object_customized(cluster, label):
    global projectitem_df
    res = False
    if projectitem_df.size == 0:
        import_customizations()
    
    if cluster == 'Component':
        print(f'Searching for {label} in {cluster}')
        res = projectitem_df[(projectitem_df['OBJECTTYPE'] == 7) & (projectitem_df['OBJECTVALUE1'] == label)].size > 0
    if cluster == 'Page':
        res = projectitem_df[(projectitem_df['OBJECTTYPE'] == 5) & (projectitem_df['OBJECTVALUE1'] == label)].size > 0
    if cluster == 'Record':
        res = projectitem_df[(projectitem_df['OBJECTTYPE'] == 0) & (projectitem_df['OBJECTVALUE1'] == label)].size > 0
    if cluster == 'Activate Events':
        page_name = label.split('.')[0]
        res = projectitem_df[(projectitem_df['OBJECTTYPE'] == 44) & (projectitem_df['OBJECTVALUE1'] == page_name)].size > 0
    if cluster == 'Component PeopleCode':
        label_split = label.split('.')
        component_name = label_split[0]
        peoplecode_event = label_split[2]
        # print(f'Component PeopleCode for {component_name} and {peoplecode_event}')
        res = projectitem_df[(projectitem_df['OBJECTTYPE'] == 46) & (projectitem_df['OBJECTVALUE1'] == component_name) & (projectitem_df['OBJECTVALUE3'] == peoplecode_event)].size > 0


    return res

# data_list [dataframe['Node','Edge'], input_type: str]
def create_dot_source(data_list, custom_obj):
    # start the object
    dot = graphviz.Digraph('PSFT Explorer', comment='A PeopleSoft Object Explorer')
    dot.format = 'svg'
    dot.attr('node', {'fontname':'Helvetica,Arial,sans-serif', 'color':'#61c2c5', 'fontsize':'9'})
    dot.attr('edge', {'arrowsize':'0.5', 'weight':'1'})
    dot.attr(rankdir='LR', center='True', ranksep='1.5')

    # style definitions
    style_dict = {
        'input_style': {'color':'plum', 'style':'filled'},
        'Component': {'shape': 'box3d', 'rank': 'same', 'fontsize': '7', 'group': 'Component'},
        'Page': {'shape': 'component', 'group': 'Page'},
        'SubPage': {'shape': 'rectangle', 'group': 'SubPage'},
        'Record': {'shape': 'cylinder', 'group': 'Record'},
        'Tables': {'shape': 'cylinder', 'group': 'Tables'},
        'Views': {'shape': 'cylinder', 'group': 'Views'},
        'Derived': {'shape': 'cylinder', 'group': 'Derived'},
        'Classes': {'shape': 'rectangle', 'color': 'lightgrey', 'rank': 'same', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Component PeopleCode': { 'color': 'green', 'style':'filled', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Component Record PeopleCode': {'color': 'blue', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Component RecField PeopleCode': {'color': 'lightblue2','fontsize': '7' ,'group': 'PeopleCode'},
        'Activate Events': {'color': 'orange', 'style':'filled', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Application Packages': {'color': 'lightblue2', 'style':'filled', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Function Calls': {'color': 'yellow', 'style':'filled', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Function Definitions': {'color': 'red', 'style':'filled', 'fontsize': '7' ,'group': 'PeopleCode'},
        'Custom': {'color': 'red', 'fontsize': '9' ,'group': 'Custom'},
    }

    # Data List:
    # # {dict_of_list_of_nodes}: Dictionaries of cluster (key) and List of Items (value)
    # # # List of Items: List of Dictionaries (name (key) and label (value))
    # # {list_of_edges}: List of List of Edges
    dict_of_clusters = data_list[0]
    list_of_list_of_edges = data_list[1]
    # Add Nodes to dot
    # print(f'shape of dict_of_clusters {dict_of_clusters.keys()}')
    # Iterate over keys (cluster)
    for cluster in dict_of_clusters:
        # print(f'cluster is {cluster}')
        # Retrieve value for current key (cluster)
        list_of_nodes = dict_of_clusters[cluster]
        
        if len(list_of_nodes) > 0:
            # print(f'{cluster} node_list has {len(list_of_nodes)} elements')
            # create a subgraph for current cluster
            with dot.subgraph(name="cluster_" + cluster) as c:
                c.attr(label=cluster)
                c.attr('node', style_dict[cluster])
                # print(f'Adding Nodes of {cluster}:')
                # Iterate over Node Dictionaries (name, label)
                for node in list_of_nodes:
                    # print(f'    Node: {node}')
                    # if node['attr']:
                    #     c.attr('node', {'class':cluster, 'color':'red'})  
                    # else:  
                    c.attr('node', {'class':cluster})
                    c.node(name=node["name"], label=node["label"], id=node["name"])

    # Add Edges to dot
    for list_of_edges in list_of_list_of_edges:
        for edge in list_of_edges:
            # print(f'edge in edge_list {edge}')
            dot.edge(edge[0], edge[1])

    return dot

def cook_data (input_type, my_input, detail_level, pcode_detail):

    data_list = []
    
    # Build Data List:
    # # Dictionaries of cluster (key) and List of Items (value)
    # # # List of Items: List of Dictionaries (name (key) and label (value))
    # # List of List of Edges
    params = [input_type, my_input, detail_level, pcode_detail]
    data_list = spaghetti_df(params)

    # select for custom objects

    return data_list


def update_graph(input_type, my_input, detail_level, pcode_detail, custom_obj):
    
    # Select and Refactor data needed according to input arguments
    data_list = spaghetti_df([input_type, my_input, detail_level, pcode_detail, custom_obj])

    # Generate graphviz Nodes, Edges and Styles
    f = create_dot_source(data_list, custom_obj)

    # with open('graphviz_src.txt', 'w') as f_source:
    #     f_source.write(f.source)

    return f





