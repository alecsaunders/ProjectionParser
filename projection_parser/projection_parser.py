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
        self.sorted_projection_col_list = []
        self.select_list = []
        self.from_database = None
        self.from_schema = None
        self.from_table = None
        self.order_by_list = []
        self.segmentation_spec = False  # False = UNSEGMENTED; True = SEGEMENTED
        self.modularhash = None  # False = HASH; True = MODULARHASH
        self.segment_columns = []
        self.group_by_columns = []
        self.offset = None
        self.ksafe = None
        self.is_lap = False

        # Topk Properties
        self.is_topk = False
        self.topk_limit = None
        self.topk_partition = ''
        self.topk_order_by = ''

        # Style settings
        self.tab_space = ' '*2
        self.table_name_with_column_name = False
        self.if_not_exists = True


    ######################
    ## PARSE PROJECTION ##
    def parse_projection(self):
        self.initial_sanitation()
        self.set_properties_from_create_line()
        self.set_projection_col_list()
        self.set_select_list()
        self.is_topk = self.is_projection_topk()
        if self.is_topk:
            self.set_topk_properties()
            self.set_ksafe_offset()
        else:
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

    def set_properties_from_create_line(self):
        script = self.proj_parts
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

        db, sch, proj = self.split_db_schema_obj(projection_name)

        self.projection_name = proj

        buddy_pattern = '\_b\d$'
        bx_search_result = re.search(buddy_pattern, projection_name, re.IGNORECASE)
        if bx_search_result:
            buddy_text = bx_search_result.group(0)
            projection_name = re.sub(buddy_pattern, '', projection_name)
            try:
                self.buddy = int(buddy_text[2:])
            except:
                pass

        db, sch, proj = self.split_db_schema_obj(projection_name)
        self.projection_database = db
        self.projection_schema = sch
        self.projection_basename = proj

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
            proj_col_dict['col_name'] = c_split[0]
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

    def remove_table_from_col(self, col):
        if len(col.split(".")) > 1:
            col = col.split(".")[1]
        return col

    def set_select_list(self):
        select_parts, self.proj_parts = re.split('\sFROM\s', self.proj_parts.strip(), re.IGNORECASE)
        select_parts = re.split('\sSELECT\s', select_parts, re.IGNORECASE)[1]
        self.select_list = list(map(lambda s: self.parser_select_parts(s), select_parts.split(',')))

    def parser_select_parts(self, column):
        select_column_dict = {}

        column = column.strip()
        column = re.split('\sAS\s', column, re.IGNORECASE)
        if '(' in column[0]:
            self.is_lap = True
            agg_func, col_name = column[0].split('(')
            agg_func = agg_func.strip()
            col_name = col_name.split(')')[0].strip()
            select_column_dict['col_name'] = col_name
            select_column_dict['agg_func'] = agg_func
        else:
            select_column_dict['col_name'] = column[0]

        if not self.table_name_with_column_name:
            select_column_dict['col_name'] = self.remove_table_from_col(select_column_dict['col_name'])

        if len(column) > 1:
            col_alias = column[1].strip()
            select_column_dict['col_alias'] = col_alias

        return select_column_dict

    def set_from_clause(self):
        from_parts, self.proj_parts = re.split('\sORDER BY\s', self.proj_parts.strip(), re.IGNORECASE)
        self.set_from_parts(from_parts)

    def set_from_parts(self, from_parts):
        from_parts = from_parts.strip()
        db, sch, table = self.split_db_schema_obj(from_parts)
        self.from_database = db
        self.from_schema = sch
        self.from_table = table

    def set_order_by_list(self):
        order_by_parts = self.proj_parts.strip()
        segmented_pattern = '\sSEGMENTED\sBY\s'
        seg_search_result = re.search(segmented_pattern, self.proj_parts.strip(), re.IGNORECASE)
        if seg_search_result:
            order_by_parts = re.split(segmented_pattern, order_by_parts, re.IGNORECASE)[0]
        order_by_parts = order_by_parts.strip().split(',')
        if not self.table_name_with_column_name:
            order_by_parts = list(map(lambda o: self.remove_table_from_col(o), order_by_parts))
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
            segment_columns_list = segment_columns.split(',')
            if not self.table_name_with_column_name:
                segment_columns_list = list(map(lambda c: self.remove_table_from_col(c), segment_columns_list))
            self.segment_columns = list(map(lambda c: c.strip(), segment_columns_list))
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

    def is_projection_topk(self):
        raw_proj_oneline = ''.join(self.raw_proj.split())
        if 'over(partitionby' in raw_proj_oneline.lower():
            return True
        else:
            return False

    def set_topk_properties(self):
        limit_pattern = re.compile('\sLIMIT\s\d+', re.IGNORECASE)
        limit = re.search(limit_pattern, self.proj_parts).group(0)
        from_parts, self.proj_parts = re.split(limit_pattern, self.proj_parts)
        self.set_from_parts(from_parts)
        self.topk_limit = limit.split()[1]
        part_order_by_pattern = re.compile('\sORDER BY\s', re.IGNORECASE)
        over_partition, order_by_parts = re.split(part_order_by_pattern, self.proj_parts)
        partition_by_pattern = re.compile('\(\s*PARTITION\sBY\s', re.IGNORECASE)
        part_clause = re.split(partition_by_pattern, over_partition)[1].strip()
        self.parse_partition(part_clause)
        end_over_paren_pattern = re.compile('\)', re.IGNORECASE)
        topk_order_by, self.proj_parts = re.split(end_over_paren_pattern, order_by_parts)
        self.parse_topk_order_by(topk_order_by)

    def parse_partition(self, partition_clause):
        col_list = self.get_col_names_only(partition_clause)
        self.topk_partition = self.single_line_column_list(col_list)

    def parse_topk_order_by(self, order_clause):
        col_list = self.get_col_names_only(order_clause)
        self.topk_order_by = self.single_line_column_list(col_list)


    ##########################
    ## RECOMPILE PROJECTION ##
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
        if self.is_lap:
            group_by_clause = self.compile_group_by_clause()
            recompiled_projection_list.append(group_by_clause)
            recompiled_projection_list.append('ALL NODES;')
        elif self.is_topk:
            limit_part_order_line = self.compile_limit_part_order()
            recompiled_projection_list.append(limit_part_order_line)
            recompiled_projection_list.append('ALL NODES;')
        else:
            order_by_clause = self.compile_order_by_clause()
            segment_clause = self.compile_segment_clause()
            recompiled_projection_list.append(order_by_clause)
            recompiled_projection_list.append(segment_clause)

        self.recompiled_projection = '\n'.join(recompiled_projection_list)

    def compile_create_line(self):
        create_line = 'CREATE PROJECTION '
        create_line = create_line + 'IF NOT EXISTS ' if self.if_not_exists else create_line
        create_line = create_line + self.projection_database + '.' if self.projection_database else create_line
        create_line = create_line + self.projection_schema + '.' if self.projection_schema else create_line
        create_line = create_line + self.projection_basename
        create_line = create_line + ' /*+createtype(' + self.create_type + ')*/' if self.create_type else create_line
        return create_line

    def compile_projection_columns(self):
        proj_col_list = ['(']
        self.sorted_projection_col_list = self.order_select_columns(self.projection_col_list)
        for count, col in enumerate(self.sorted_projection_col_list, 1):
            formatted_column = self.format_projection_column(col, count)
            proj_col_list.append(formatted_column)
        proj_col_list.append(')')
        proj_col_list.append('AS')

        projection_columns = '\n'.join(proj_col_list)
        return projection_columns

    def format_projection_column(self, col, count):
        column_str = self.tab_space + col['col_name']
        column_str = column_str + ' ENCODING ' + col['encoding'] if col['encoding'] else column_str
        column_str = column_str + ' ACCESSRANK ' + str(col['accessrank']) if col['accessrank'] else column_str
        column_str = column_str + ',' if count < len(self.sorted_projection_col_list) else column_str
        return column_str

    def compile_select_columns(self):
        select_line = 'SELECT'
        select_col_list = [select_line]
        ordered_select_list = self.order_select_columns(self.select_list)
        for count, col in enumerate(ordered_select_list, 1):
            select_column_string = self.format_string_column(col)
            formatted_column = self.format_generic_column(select_column_string, count, len(ordered_select_list))
            select_col_list.append(formatted_column)

        select_columns = '\n'.join(select_col_list)
        return select_columns

    def order_select_columns(self, select_list):
        ordered_list = []
        remaining_list = []
        for ob_col in self.order_by_list:
            for sel_col in select_list:
                if sel_col['col_name'] == ob_col:
                    ordered_list.append(sel_col)

        remaining_list = select_list
        for ob_col in ordered_list:
            for i in range(len(remaining_list) -1, -1, -1):
                if ob_col['col_name'] == remaining_list[i]['col_name']:
                    del remaining_list[i]

        return ordered_list + remaining_list

    def format_string_column(self, col_dict):
        if 'agg_func' in col_dict.keys():
            col_name = '{0}({1})'.format(col_dict['agg_func'].upper(), col_dict['col_name'])
        else:
            col_name = col_dict['col_name']
        if 'col_alias' in col_dict.keys():
            col_name = '{0} AS {1}'.format(col_name, col_dict['col_alias'])
        return col_name

    def format_column_list(self, col_name_list, delim, indent_cnt):
        formated_columns = delim.join(list(map(lambda c: self.tab_space*indent_cnt + c + ',', col_name_list)))[:-1]
        return formated_columns

    def format_generic_column(self, col, count, list_len):
        column_str = self.tab_space + col
        column_str = column_str + ',' if count < list_len else column_str
        return column_str

    def compile_from_cluase(self):
        from_clause = 'FROM '
        from_clause = from_clause + self.from_database + '.' if self.from_database else from_clause
        from_clause = from_clause + self.from_schema + '.' if self.from_schema else from_clause
        from_clause = from_clause + self.from_table
        return from_clause

    def compile_group_by_clause(self):
        group_by_column_list = list(filter(None, map(lambda c: c['col_name'] if not 'agg_func' in c else None, self.select_list)))
        group_by_columns = self.format_column_list(group_by_column_list, '\n', 2)

        group_by_section = self.tab_space + 'GROUP BY\n{}'.format(group_by_columns)
        return group_by_section

    def compile_order_by_clause(self):
        order_by_line = 'ORDER BY'
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
        proj_segment_columns = self.segment_columns
        if not self.table_name_with_column_name:
            proj_segment_columns = list(map(lambda c: self.strip_segment_columns(c), proj_segment_columns))

        segment_columns = ', '.join(proj_segment_columns)
        segment_clause = segment_clause + segment_columns + ') ALL NODES'
        ### If you want to set K-Safety or Offset, uncomment the next two lines
        # segment_clause = segment_clause + ' KSAFE ' + str(self.ksafe) if self.ksafe else segment_clause
        # segment_clause = segment_clause + ' OFFSET ' + str(self.offset) if self.offset else segment_clause
        segment_clause = segment_clause + ';'
        return segment_clause

    def strip_segment_columns(self, col):
        col_parts = col.split('.')
        stripped_column_name = col_parts[len(col_parts) - 1]
        return stripped_column_name

    def compile_limit_part_order(self):
        lpo_line = '{0} {1} {2} {3} {4} {5})'.format(
            'LIMIT',
            self.topk_limit,
            'OVER(PARTITION BY',
            self.topk_partition,
            'ORDER BY',
            self.topk_order_by,
        )
        return lpo_line


    ##########################
    ## UTILITY FUNCTIONS    ##

    def split_db_schema_obj(self, full_obj):
        db = None
        schema = None
        obj = None
        if '.' in full_obj:
            obj_parts = full_obj.split('.')
            if len(obj_parts) == 2:
                schema = obj_parts[0]
                obj = obj_parts[1]
            if len(obj_parts) == 3:
                db = obj_parts[0]
                schema = obj_parts[1]
                obj = obj_parts[2]
        else:
            obj = full_obj

        return db, schema, obj

    def get_col_names_only(self, full_col_name):
        columns = []
        new_columns = []

        if ',' in full_col_name:
            columns = full_col_name.split(',')
        else:
            columns = [full_col_name]

        for c in columns:
            if '.' in c:
                c_parts = c.split('.')
                col_name = c_parts[len(c_parts) - 1].strip()
                new_columns.append(col_name)
        return new_columns

    def single_line_column_list(self, col_list):
        return ', '.join(col_list)
