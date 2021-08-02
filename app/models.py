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
    # dimension_attributes = models.ForeignKey(DimensionAttribute)
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
