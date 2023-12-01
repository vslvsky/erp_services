from . import DataBase_rj as dbase

# Класс для загрузки данных:
class MessengerSQL:
    LIMIT_INSERT = 1000

    def __init__(self, sql_obj):
        self.sql_obj = sql_obj
        self.answer = None
        self.connector = None

    def connect(self):
        if self.sql_obj:
            self.connector = self.sql_obj.set_connections()

    def collect_command(self, structure, data_list):#Генерация запроса с множественной вставкой
        column_count = len(data_list[0])
        strings_count = len(data_list)
        caret_char = '%s'
        strings_command = ', '.join([caret_char] * column_count)
        insert_command = ', '.join([f'({strings_command})'] * strings_count)
        out_command = structure.format(insert_here=insert_command)
        return out_command

    def straighten_data(self, data_list):#Двухмерный массив в одномерный
        return [[i for j in data_list for i in j]]

    def send_command(self, command):
        if command and self.connector:
            return dbase.Relational_DB.get_dataframe(command, self.connector)

    def send_command_no_data(self, command):
        if command and self.connector:
            return dbase.Relational_DB.sql_commands(command, self.connector)

    def execute_method_slow(self, command, data_frame):#Старый очень медленный метод
        if self.connector and command:
            dbase.Relational_DB.insert_dataframe(command, self.connector, data_frame)

    def execute_method(self, command, data_frame):#Метод с множественной вставкой
        part_data_list = []
        if self.connector and command:
            index = 0
            for data_line in data_frame.values:
                part_data_list.append(data_line)
                index += 1
                if index >= self.LIMIT_INSERT:
                    self._send_data_execute(command, part_data_list)
                    part_data_list = []
                    index = 0
            else:
                if index > 0:
                    self._send_data_execute(command, part_data_list)

    def _send_data_execute(self, command, data_list):
        command_insert = self.collect_command(command, data_list)
        data_insert = self.straighten_data(data_list)
        #print(data_insert)
        self.connector[1].executemany(command_insert, data_insert)
        self.connector[0].commit()