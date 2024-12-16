import mysql.connector
from mysql.connector import Error
import hashlib

def connect_mysql_db():
    host = ""
    user = ""  
    password = ""
    database = ""

    hashed_host = hashlib.sha256(host.encode()).hexdigest()
    hashed_user = hashlib.sha256(user.encode()).hexdigest()
    hashed_database = hashlib.sha256(database.encode()).hexdigest()
    hashed_password = hashlib.sha256(password.encode()).hexdigest()

    print(f"Hash del host: {hashed_host}")
    print(f"Hash del usuario: {hashed_user}")
    print(f"Hash de la base de datos: {hashed_database}")
    print(f"Hash de la contraseña: {hashed_password}")



    try:
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("Conexión exitosa a la base de datos")

    except Error as e:
        print(f"Error al conectar con MySQL: {e}")
        return None
    
    return (connection)

def create_table_mysql(connection_param, table_name_param, data_columns_param):
    my_cursor = connection_param.cursor()

    my_cursor.execute(f"SHOW TABLES LIKE '{table_name_param}'")
    result = my_cursor.fetchone()
    
    data_columns = data_columns_param

    column_definitions = ",".join([f"{value} VARCHAR(255)" for value in data_columns[1:]] )

    if result is None:
        insert_query =f'''
            CREATE TABLE {table_name_param} (
                {data_columns[0]} INT AUTO_INCREMENT PRIMARY KEY,
                {column_definitions}
            )
        '''

        my_cursor.execute(insert_query)
        print(f"Tabla {table_name_param} creada exitosamente.")
    else:
        print(f"La tabla {table_name_param} ya existe.")

    return

def insert_data_in_mysql(connection_insert_param, data_insert_param, table_name_param):
    my_insert_cursor = connection_insert_param.cursor()
    try:
        for _, row in data_insert_param.iterrows():
            new_data = row.to_list()  #Utilizando Listas
            #new_data = row.to_dict()  #Utilizanco Diccionario

            data_without_empty = []
            for elements in new_data:
                if not elements:  
                    data_without_empty.append('NULL')
                else:
                    data_without_empty.append(elements)

            insert_query = f"""
                INSERT INTO {table_name_param}(id, name, type, dimension, residents, url, created) 
                VALUES ({data_without_empty[0]},"{data_without_empty[1]}","{data_without_empty[2]}","{data_without_empty[3]}","{data_without_empty[4]}","{data_without_empty[5]}","{data_without_empty[6]}")
                
            """    
               
            my_insert_cursor.execute(insert_query)
            connection_insert_param.commit()

    except Error as e:
        print(f"Error al conectar con MySQL: {e}")
        return None
    
    return (print("Datos insertados correctamente en la tabla."))

def close_mysql_connection(connection_param):
    
    try:
        if connection_param.is_connected():
                connection_param.close()

    except Error as e:

        print(f"Error al cerrar conexion de MySQL: {e}")
        return None         
    return(print("Conexión cerrada"))

