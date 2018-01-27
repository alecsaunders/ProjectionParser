import re

class ProjParser():
    def __init__(self):
        self.recompiled_projection = None
        self.raw_proj = None
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
        self.segment_columns = []
        self.group_by_column = []
        self.offset = None
        self.ksafe = None
        self.lap = False

        # Style settings
        self.tab_space = ' '*8

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
        self.proj_parts = self.raw_proj.strip()
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
            self.parse_hints(hint)
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

    def parse_hints(self, hint):
        hints = self.get_hint_list(hint)
        self.set_hint_parts(hints)

    def get_hint_list(self, hint):
        comment_start_pattern = '^\/\*\+'
        comment_end_pattern = '\*\/$'
        hint = re.sub(comment_start_pattern, '', hint)
        hint = re.sub(comment_end_pattern, '', hint)
        if ',' in hint:
            return hint.split(',')
        else:
            return [hint]

    def set_hint_parts(self, hints):
        for hint in hints:
            hint = hint.strip()
            if re.search('createtype', hint, re.IGNORECASE):
                hint = re.sub('createtype', '', hint, re.IGNORECASE).strip()
                hint = hint.replace('(', '').replace(')', '')
                self.create_type = hint.strip()

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
        self.select_list = list(map(lambda s: self.parser_select_parts(s), select_parts.split(',')))

    def parser_select_parts(self, column):
        select_column_dict = {}

        column = column.strip()
        column = re.split('\sAS\s', column, re.IGNORECASE)
        if '(' in column[0]:
            self.lap = True
            agg_func, col_name = column[0].split('(')
            agg_func = agg_func.strip()
            col_name = col_name.split(')')[0].strip()
            select_column_dict['col_name'] = col_name
            select_column_dict['agg_func'] = agg_func
        else:
            select_column_dict['col_name'] = column[0]

        if len(column) > 1:
            col_alias = column[1].strip()
            select_column_dict['col_alias'] = col_alias

        return select_column_dict

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
        order_by_parts = self.proj_parts.strip()
        segmented_pattern = '\sSEGMENTED\sBY\s'
        seg_search_result = re.search(segmented_pattern, self.proj_parts.strip(), re.IGNORECASE)
        if seg_search_result:
            order_by_parts = re.split(segmented_pattern, order_by_parts, re.IGNORECASE)[0]
        order_by_parts = order_by_parts.strip().split(',')
        self.order_by_list = list(map(lambda o: re.split('\s', o.strip())[0], order_by_parts))

    def set_segmentation_clause(self):
        rep_pattern = '\sUNSEGMENTED\s'
        rep_search_result = re.search(rep_pattern, self.proj_parts.strip(), re.IGNORECASE)
        if not rep_search_result:
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
            hash_type, segment_columns = hash_parts.split('(')
            self.set_hash_type(hash_type)
            self.set_segment_columns(segment_columns)


    def set_hash_type(self, hash_type):
        hash_type = hash_type.strip()
        mod_hash_pattern = re.compile('^MODULARHASH$', re.IGNORECASE)
        if mod_hash_pattern.match(hash_type):
            self.modularhash = True
        else:
            hash_pattern = re.compile('^HASH$', re.IGNORECASE)
            if hash_pattern.match(hash_type):
                self.modularhash = False

    def set_segment_columns(self, segment_columns):
        segment_columns = segment_columns.strip()
        if ',' in segment_columns:
            self.segment_columns = list(map(lambda c: c.strip(), segment_columns.split(',')))
        else:
            self.segment_columns.append(segment_columns)

    def set_ksafe_offset(self):
        ksafe_pattern = re.compile('KSAFE\s', re.IGNORECASE)
        ksafe_search_result = re.search(ksafe_pattern, self.proj_parts)
        if ksafe_search_result:
            ksafe_parts = re.split(ksafe_pattern, self.proj_parts)[1]
            self.ksafe = int(ksafe_parts.split(' ')[0].strip())

        offset_pattern = re.compile('OFFSET\s', re.IGNORECASE)
        offset_search_result = re.search(offset_pattern, self.proj_parts)
        if offset_search_result:
            offset_parts = re.split(offset_pattern, self.proj_parts)[1]
            self.offset = int(offset_parts.split(' ')[0].strip())

    def recompile_projection(self):
        recompiled_projection_list = []
        create_line = self.compile_create_line()
        projection_columns = self.compile_projection_columns()
        select_clause = self.compile_select_columns()
        from_clause = self.compile_from_cluase()


        recompiled_projection_list.append(create_line)
        recompiled_projection_list.append(projection_columns)
        recompiled_projection_list.append(select_clause)
        recompiled_projection_list.append(from_clause)
        if self.lap:
            group_by_clause = self.compile_group_by_clause()
            recompiled_projection_list.append(group_by_clause)
        else:
            order_by_clause = self.compile_order_by_clause()
            segment_clause = self.compile_segment_clause()
            recompiled_projection_list.append(order_by_clause)
            recompiled_projection_list.append(segment_clause)

        self.recompiled_projection = '\n'.join(recompiled_projection_list)

    def compile_create_line(self):
        create_line = 'CREATE PROJECTION '
        create_line = create_line + self.projection_database + '.' if self.projection_database else create_line
        create_line = create_line + self.projection_schema + '.' if self.projection_schema else create_line
        create_line = create_line + self.projection_basename
        create_line = create_line + ' /*+createtype(' + self.create_type + ')*/' if self.create_type else create_line
        return create_line

    def compile_projection_columns(self):
        proj_col_list = ['(']
        for count, col in enumerate(self.projection_col_list, 1):
            formatted_column = self.format_projection_column(col, count)
            proj_col_list.append(formatted_column)
        proj_col_list.append(')')
        proj_col_list.append('AS')

        projection_columns = '\n'.join(proj_col_list)
        return projection_columns

    def format_projection_column(self, col, count):
        column_str = self.tab_space + col['projection_col']
        column_str = column_str + ' ENCODING ' + col['encoding'] if col['encoding'] else column_str
        column_str = column_str + ' ACCESSRANK ' + str(col['accessrank']) if col['accessrank'] else column_str
        column_str = column_str + ',' if count < len(self.projection_col_list) else column_str
        return column_str

    def compile_select_columns(self):
        select_line = self.tab_space + 'SELECT'
        select_col_list = [select_line]
        for count, col in enumerate(self.select_list, 1):
            select_column_string = self.format_string_column(col)
            formatted_column = self.format_generic_column(select_column_string, count, len(self.select_list))
            select_col_list.append(formatted_column)

        select_columns = '\n'.join(select_col_list)
        return select_columns

    def format_string_column(self, col_dict):
        if 'agg_func' in col_dict.keys():
            col_name = '{0}({1})'.format(col_dict['agg_func'].upper(), col_dict['col_name'])
        else:
            col_name = col_dict['col_name']
        if 'col_alias' in col_dict.keys():
            col_name = '{0} AS {1}'.format(col_name, col_dict['col_alias'])
        return col_name

    def format_generic_column(self, col, count, list_len):
        column_str = self.tab_space*2 + col
        column_str = column_str + ',' if count < list_len else column_str
        return column_str

    def compile_from_cluase(self):
        from_clause = self.tab_space + 'FROM '
        from_clause = from_clause + self.from_database + '.' if self.from_database else from_clause
        from_clause = from_clause + self.from_schema + '.' if self.from_schema else from_clause
        from_clause = from_clause + self.from_table
        return from_clause

    def compile_group_by_clause(self):
        group_by_line = self.tab_space + 'GROUP BY'
        return group_by_line

    def compile_order_by_clause(self):
        order_by_line = self.tab_space + 'ORDER BY'
        order_list = [order_by_line]
        for count, col in enumerate(self.order_by_list, 1):
            formatted_column = self.format_generic_column(col, count, len(self.order_by_list))
            order_list.append(formatted_column)

        order_by_columns = '\n'.join(order_list)
        return order_by_columns

    def compile_segment_clause(self):
        if not self.segmentation_spec:
            return 'UNSEGMENTED ALL NODES;'
        else:
            segment_clause = self.compile_segment_parts()
            return segment_clause

    def compile_segment_parts(self):
        segment_clause = 'SEGMENTED BY '
        segment_clause = segment_clause + 'MODULARHASH(' if self.modularhash == True else segment_clause
        segment_clause = segment_clause + 'HASH(' if self.modularhash == False else segment_clause
        segment_columns = ', '.join(self.segment_columns)
        segment_clause = segment_clause + segment_columns + ') ALL NODES'
        # segment_clause = segment_clause + ' KSAFE ' + str(self.ksafe) if self.ksafe else segment_clause
        # segment_clause = segment_clause + ' OFFSET ' + str(self.offset) if self.offset else segment_clause
        segment_clause = segment_clause + ';'
        return segment_clause
