import re

class ProjectionObject():
    def __init__(self, create_projection_statement):
        self.raw_proj = create_projection_statement
        self.proj_parts = self.raw_proj
        self.projection_database = None
        self.projection_schema = None
        self.projection_basename = None
        self.buddy = None
        self.create_type = None
        self.projection_col_list = []
        self.select_list = []
        self.from_database = None
        self.from_schema = None
        self.from_table = None
        self.order_by_list = []
        self.segmentation_spec = False  # False = UNSEGMENTED; True = SEGEMENTED
        self.modularhash = None  # False = HASH; True = MODULARHASH
        self.hash_columns = []
        self.offset = None
        self.ksafe = None

    def parse_projection(self):
        self.initial_sanitation()
        self.set_projection_properties()
        self.set_projection_col_list()
        self.set_select_list()
        self.set_from_clause()
        self.set_order_by_list()
        self.set_segmentation_clause()
        self.proj_parts = self.raw_proj

    def initial_sanitation(self):
        self.proj_parts = self.proj_parts.strip()
        if self.proj_parts.endswith(';'):
            self.proj_parts = self.proj_parts[:-1]
        while '  ' in self.proj_parts:
            self.proj_parts = self.proj_parts.replace('  ', ' ')

    def set_projection_properties(self):
        self.set_properties_from_create_line(self.proj_parts)

    def set_properties_from_create_line(self, script):
        hint_pattern = '\/\*.*\*\/'
        hint_search_result = re.search(hint_pattern, script)
        if hint_search_result:
            hint = hint_search_result.group(0)
            self.set_create_type(hint)
            self.proj_parts = re.sub(hint_pattern, '', script)
        create_line = self.proj_parts.split('(')[0].strip()
        create_projection_pattern = re.compile('CREATE PROJECTION ', re.IGNORECASE)
        if_not_exists_pattern = re.compile('IF NOT EXISTS ', re.IGNORECASE)
        create_line = re.sub(create_projection_pattern, '', create_line)
        create_line = re.sub(if_not_exists_pattern, '', create_line)
        projection_name = create_line.strip()

        buddy_pattern = '\_b\d$'
        bx_search_result = re.search(buddy_pattern, projection_name, re.IGNORECASE)
        if bx_search_result:
            buddy_text = bx_search_result.group(0)
            projection_name = re.sub(buddy_pattern, '', projection_name)
            try:
                self.buddy = int(buddy_text[2:])
            except:
                pass

        if '.' in projection_name:
            proj_parts = projection_name.split('.')
            if len(proj_parts) == 2:
                self.projection_schema = proj_parts[0]
                self.projection_basename = proj_parts[1]
            if len(proj_parts) == 3:
                self.projection_database = proj_parts[0]
                self.projection_schema = proj_parts[1]
                self.projection_basename = proj_parts[2]
        else:
            self.projection_basename = projection_name

    def set_create_type(self, hint):
        hint = hint.replace('/*+', '').replace('/*', '').replace('*/', '').strip()
        create_type_pattern = re.compile('createtype\(', re.IGNORECASE)
        create_type_search_result = re.search(create_type_pattern, hint)
        if create_type_search_result:
            create_type = re.sub(create_type_pattern, '', hint).strip()
            create_type = create_type.split(')')[0].strip()
            self.create_type = create_type

    def set_projection_col_list(self):
        projection_cols_split = self.proj_parts.split(')')
        projection_cols_raw = projection_cols_split[0]
        self.proj_parts = ')'.join(projection_cols_split[1:])
        projection_cols_raw = projection_cols_raw.split('(')[1].strip()
        projection_cols_raw_list = projection_cols_raw.split(',')
        projection_cols_raw_list = list(map(lambda c: c.strip(), projection_cols_raw_list))

        for c in projection_cols_raw_list:
            proj_col_dict = {}
            c_split = c.split(' ')
            proj_col_dict['projection_col'] = c_split[0]
            proj_column_modifiers = ' '.join(c_split[1:]).strip().upper()
            if 'ENCODING ' in proj_column_modifiers:
                proj_col_dict['encoding'] = proj_column_modifiers.split('ENCODING ')[1].strip().split(' ')[0]
            else:
                proj_col_dict['encoding'] = None
            if 'ACCESSRANK ' in proj_column_modifiers:
                proj_col_dict['accessrank'] = int(proj_column_modifiers.split('ACCESSRANK ')[1].strip())
            else:
                proj_col_dict['accessrank'] = None

            self.projection_col_list.append(proj_col_dict)

    def set_select_list(self):
        select_parts, self.proj_parts = re.split('\sFROM\s', self.proj_parts.strip(), re.IGNORECASE)
        select_parts = re.split('\sSELECT\s', select_parts, re.IGNORECASE)[1]
        self.select_list = list(map(lambda s: s.strip(), select_parts.split(',')))

    def set_from_clause(self):
        from_parts, self.proj_parts = re.split('\sORDER BY\s', self.proj_parts.strip(), re.IGNORECASE)
        from_parts = from_parts.strip()
        if '.' in from_parts:
            from_parts = from_parts.split('.')
            if len(from_parts) == 2:
                self.from_schema = from_parts[0]
                self.from_table = from_parts[1]
            if len(from_parts) == 3:
                self.from_database = from_parts[0]
                self.from_schema = from_parts[1]
                self.from_table = from_parts[2]
        else:
            self.from_table = from_parts

    def set_order_by_list(self):
        order_by_parts = self.proj_parts.strip().split(',')
        self.order_by_list = list(map(lambda o: re.split('\s', o.strip())[0], order_by_parts))

    def set_segmentation_clause(self):
        seg_pattern = '\sUNSEGMENTED\s'
        seg_search_result = re.search(seg_pattern, self.proj_parts.strip(), re.IGNORECASE)
        if not seg_search_result:
            self.segmentation_spec = True
            self.set_segmentation_parts()

    def set_segmentation_parts(self):
        self.proj_parts = re.split('\sSEGMENTED BY\s', self.proj_parts, re.IGNORECASE)[1].strip()
        self.set_hash_parts()
        self.set_ksafe_offset()


    def set_hash_parts(self):
        hash_search_result = re.search('HASH', self.proj_parts, re.IGNORECASE)
        if hash_search_result:
            hash_parts, self.proj_parts = self.proj_parts.split(')')
            hash_type, hash_columns = hash_parts.split('(')
            self.set_hash_type(hash_type)
            self.set_hash_columns(hash_columns)


    def set_hash_type(self, hash_type):
        hash_type = hash_type.strip()
        mod_hash_pattern = re.compile('^MODULARHASH$', re.IGNORECASE)
        if mod_hash_pattern.match(hash_type):
            self.modularhash = True
        else:
            hash_pattern = re.compile('^HASH$', re.IGNORECASE)
            if hash_pattern.match(hash_type):
                self.modularhash = False

    def set_hash_columns(self, hash_columns):
        hash_columns = hash_columns.strip()
        if ',' in hash_columns:
            self.hash_columns = list(map(lambda c: c.strip(), hash_columns.split(',')))
        else:
            self.hash_columns.append(hash_columns)

    def set_ksafe_offset(self):
        ksafe_pattern = 'KSAFE\s'
        ksafe_search_result = re.search(ksafe_pattern, self.proj_parts, re.IGNORECASE)
        if ksafe_search_result:
            ksafe_parts = re.split(ksafe_pattern, self.proj_parts, re.IGNORECASE)[1]
            self.ksafe = int(ksafe_parts.split(' ')[0].strip())

        offset_pattern = 'OFFSET\s'
        offset_search_result = re.search(offset_pattern, self.proj_parts, re.IGNORECASE)
        if offset_search_result:
            offset_parts = re.split(offset_pattern, self.proj_parts, re.IGNORECASE)[1]
            self.offset = int(offset_parts.split(' ')[0].strip())


if __name__ == '__main__':
    proj_script = open('design', 'r').read()
    proj_obj = ProjectionObject(proj_script)
    proj_obj.parse_projection()

    for key, value in proj_obj.__dict__.iteritems():
        print(key)
        print(value)
        print('')
