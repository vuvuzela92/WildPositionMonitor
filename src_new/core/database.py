import os
from sqlalchemy import create_engine, Column, MetaData, Table, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class Database:
    _engine = None
    _SessionFactory = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            url = URL.create(
                drivername="postgresql",
                username=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                database=os.getenv("POSTGRES_DB"),
            )

            cls._engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20,
                echo=False,
            )

        return cls._engine

    @classmethod
    def get_session(cls):
        if cls._SessionFactory is None:
            engine = cls.get_engine()
            cls._SessionFactory = sessionmaker(bind=engine)

        return cls._SessionFactory()
    
    @classmethod
    def read_sql_to_dataframe(cls, query, params=None):
        """
        Выполняет SQL-запрос и возвращает результат в датафрейме.
        """
        with cls.get_engine().connect() as connection:
            return pd.read_sql(query, connection, params=params)
        
    @classmethod
    def read_sql_to_dict(cls, query, params=None):
        """
        Выполняет SQL-запрос и возвращает результат в виде списка словарей.
        """
        with cls.get_engine().connect() as connection:
            # Оборачиваем строковый запрос в text() для безопасности и совместимости
            result = connection.execute(text(query), params or {})
            
            # Превращаем результат в список словарей
            # mappings() позволяет обращаться к полям по именам колонок
            return [dict(row) for row in result.mappings()]
        
    @classmethod
    def read_sql_to_list(cls, query, params=None):
        """
        Выполняет SQL-запрос и возвращает результат в виде списка словарей.
        """
        with cls.get_engine().connect() as connection:
            # Оборачиваем строковый запрос в text() для безопасности и совместимости
            result = connection.execute(text(query), params or [])
            
            # Превращаем результат в список словарей
            # mappings() позволяет обращаться к полям по именам колонок
            return [list(row) for row in result.mappings()]

    @classmethod
    def sync_data_to_postgres(cls, table_name, data, schema_definition, unique_keys, chunk_size=30000):
        """Синхронизирует данные с PostgreSQL, выполняя UPSERT по уникальным ключам."""
        engine = cls.get_engine()
        metadata = MetaData()

        columns = []
        for col_name, col_type in schema_definition.items():
            columns.append(Column(col_name, col_type))

        if unique_keys:
            columns.append(UniqueConstraint(*unique_keys, name=f"uq_{table_name}_keys"))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)

        if data is None or (hasattr(data, 'empty') and data.empty):
            return

        if hasattr(data, "to_dict"):
            data_to_insert = data.to_dict(orient="records")
        else:
            data_to_insert = data

        # 🔥 РАЗБИВАЕМ НА ЧАНКИ
        total = len(data_to_insert)
        inserted = 0
        
        with engine.begin() as conn:
            for i in range(0, total, chunk_size):
                chunk = data_to_insert[i:i + chunk_size]
                
                stmt = insert(table).values(chunk)
                
                # update_cols = {
                #     col.name: col
                #     for col in stmt.excluded
                #     if col.name not in unique_keys
                # }
                available_cols = set(chunk[0].keys()) if chunk else set()
                update_cols = {
                    col.name: getattr(stmt.excluded, col.name)
                    for col in table.c
                    if col.name not in unique_keys and col.name in available_cols
                }

                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=unique_keys,
                    set_=update_cols,
                )

                conn.execute(upsert_stmt)
                inserted += len(chunk)
                print(f"[OK] Успешно {i}-{i+len(chunk)} из {total} синхронизировано")
        
        print(f" Таблица '{table_name}': успешно синхронизирована")
        print(f" Всего обработано {inserted} строк.")