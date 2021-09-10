# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import io
import pygrametl
import psycopg2
from django.core.files.storage import FileSystemStorage
from django.db import models
from django.contrib.auth.models import User
from pygrametl.datasources import CSVSource, MergeJoiningSource, TransformingSource
from pygrametl.tables import Dimension

# Create your models here.

fs = FileSystemStorage(location='app/uploads')


class BaseFlow(models.Model):
    # needs:
    # connection, data source, dimension
    connection_str = models.CharField(max_length=9999)
    data_source = models.FileField(upload_to='app/uploads')
    dimension_name = models.CharField(max_length=9999)
    dimension_lookup_attributes = models.CharField(max_length=9999)

    def get_conn_wrapper(self):
        dw_pgconn = psycopg2.connect(self.connection_str)
        conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_pgconn)
        return conn_wrapper

    def get_csv_source_handle(self):
        source_file_handle = io.open(self.data_source.path, 'r', 16384, encoding='utf-8-sig')
        return source_file_handle

    def conn_commit_close(self):
        self.conn_wrapper.commit()
        self.conn_wrapper.close()


class DwInfo(models.Model):
    user = models.ForeignKey(User, null=True, on_delete=models.CASCADE)
    host = models.CharField(max_length=100)
    dbname = models.CharField(max_length=100)
    db_user = models.CharField(max_length=100)
    db_password = models.CharField(max_length=100)
    db_port = models.CharField(max_length=100)

    def __str__(self):
        return self.dbname

    def get_conn_string(self):
        conn_string = f"""host='{self.host}' dbname='{self.dbname}' user='{self.db_user}' password='{self.db_password}'\
         port='{self.db_port}'"""
        return conn_string
