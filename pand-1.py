import sqlite3
import pandas as pd

conn = sqlite3.connect('database.db')
cursor=conn.cursor()

def csv2sql (filePath, tmp_auto):
    df= pd.read_csv(filePath)
    df.to_sql(tmp_auto, con=conn, if_exists = "replace",index=False)

def sql2csv (filePath, tmp_auto):
    df=pd.read_sql_query(con=conn, sql=f'SELECT * FROM {tmp_auto}')
    df.to_csv(filePath,index=False )

def showTable(tableName):
    cursor.execute(f'SELECT * FROM {tableName}')
    print('_-' *  10)
    print (tableName)
    print('_-' *  10)
    for row in cursor.fetchall():
        print(row)
    print('_-' *  10)

def init():
    cursor.execute('''
        --sql          
        CREATE TABLE if not exists hist_auto(
            id integer primary key autoincrement,
            model varchar(128),
            transmission varchar(128),
            body_type varchar(128),
            drive_type varchar(128),
            color varchar(128),
            production_year integer,
            auto_key integer,
            engine_capacity number(2,1),
            horsepower integer,
            engine_type varchar(128),
            price integer,
            milage integer,
            del_flg integer default 0,
            start_ddtm datetime default current_timestamp,
            end_dttm datetime default (datetime('2999-12-31 23:59:59')) 
        )
    ''')
    cursor.execute('''
        CREATE VIEW if NOT EXISTS v_auto as 
        SELECT
            id,
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage
        FROM hist_auto
        WHERE current_timestamp BETWEEN start_ddtm AND end_dttm
        and del_flg=0
    ''')
    
def newRows():
    cursor.execute('''
        --sql
        CREATE TABLE tmp_new_rows AS 
                   SELECT 
                        *
                   FROM tmp_auto 
                  where auto_key not in ( select auto_key from v_auto)       
    ''')


# то есть все из hist которых нет в tmp  
def deletedRows():
    cursor.execute('''   
            CREATE TABLE tmp_del_rows AS 
                   SELECT 
                        *
                   FROM v_auto
                   where auto_key not in(select auto_key from tmp_auto) 
                   
    ''')

# записи измененными данными то есть и там и там

def changeRows():
    cursor.execute('''
        CREATE TABLE tmp_change_rows AS 
                   SELECT 
                        t1.*
                   FROM tmp_auto t1 
                   INNER JOIN v_auto t2
                   ON t1.auto_key=t2.auto_key
                   where t1.model   <> t2.model     
                    OR t1.transmission      <>  t2.transmission 
                    OR t1.body_type         <>  t2.body_type 
                    OR t1.drive_type        <>  t2.drive_type 
                    OR t1.color             <>  t2.color 
                    OR t1.production_year   <>  t2.production_year 
                    OR t1.engine_capacity   <>  t2.engine_capacity 
                    OR t1.horsepower        <>  t2.horsepower 
                    OR t1.engine_type       <>  t2.engine_type
                    OR t1.price             <>  t2.price  
                    OR t1.milage            <>  t2.milage 

    ''')
    
# добавили новые записи
def change_auto_hist():
    # обьявл кот были удалены заменить end_dttm  на текущ момент
    
    cursor.execute('''
        INSERT INTO  hist_auto (
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage       
        )  
        SELECT  
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage       
        FROM tmp_new_rows
    ''')

    # у измен обьявл изменить enddttm на текущ мом времени
    # добав измен записи в hist_auto

    cursor.execute('''
        UPDATE hist_auto
        SET end_dttm = DATETIME('now','-1 second') 
        WHERE auto_key IN (
            SELECT 
                auto_key
            FROM tmp_change_rows)
            AND end_dttm = DATETIME('2999-12-31 23:59:59') 
    ''')

    
                   
    cursor.execute('''
        INSERT INTO  hist_auto (
           
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage       
        )  
        SELECT  
            
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage       
        FROM tmp_change_rows
    ''')

   
    # добавить удал записи в hist_auto  и указать флаг удаления равным 1
    cursor.execute('''
        UPDATE hist_auto
        SET end_dttm = DATETIME('now','-1 second') 
        WHERE auto_key IN (
            SELECT 
                auto_key
            FROM tmp_del_rows)
            AND end_dttm = DATETIME('2999-12-31 23:59:59') 
    ''')        
    
    cursor.execute('''
        INSERT INTO  hist_auto (
           
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage,
            del_flg     
        )  
        SELECT 
           
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage,
            1    
        FROM tmp_del_rows
    ''')
    conn.commit()
    

 

# создать функцию кот удаляет все временные таблицы
def delete_tmp_tables():
    cursor.execute('DROP TABLE if exists tmp_auto')
    cursor.execute('DROP TABLE if exists tmp_new_rows')
    cursor.execute('DROP TABLE if exists tmp_del_rows')
    cursor.execute('DROP TABLE if exists tmp_change_rows')

delete_tmp_tables()
init()
csv2sql('store/data_3.csv','tmp_auto')
newRows()
deletedRows()
changeRows()
change_auto_hist()
#sql2csv('test.csv','tmp_auto')
showTable('tmp_auto')
showTable('tmp_new_rows')
showTable('tmp_del_rows')
showTable('tmp_change_rows')
showTable('hist_auto')

