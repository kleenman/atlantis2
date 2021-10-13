# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import asyncio
import csv
import io
import os
import json

import pygrametl
import psycopg2
import pandas as pd
from django.utils.datastructures import MultiValueDictKeyError
from pygrametl.datasources import CSVSource, MergeJoiningSource, TransformingSource, PandasSource
from pygrametl.tables import Dimension
from app.utils import double_quote, TableFactory, fix_numbers
from time import sleep
from django_globals import globals
from django.core.files.storage import FileSystemStorage
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.template import loader
from django.http import HttpResponse, HttpResponseRedirect, StreamingHttpResponse
from django import template
from .forms import BaseFlowForm, FileUploadForm, DwInfoForm
from .models import BaseFlow, DwInfo

fs = FileSystemStorage(location='app/uploads')
upload_dir = fs.location


@login_required(login_url="/login/")
def home(request):
    context = {}
    context['segment'] = 'index'

    html_template = loader.get_template('index.html')
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def pages(request):
    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split('/')[-1]
        context['segment'] = load_template

        html_template = loader.get_template(load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:
        html_template = loader.get_template('page-500.html')
        return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def etl_setup(request):
    delimiter = request.session['delimiter']
    data_path = request.session['data_path']
    file_handle = io.open(data_path, 'r', 16394, encoding='utf-8-sig')
    csv_source = CSVSource(file_handle, delimiter=delimiter)

    col_names = list(csv_source.fieldnames)
    key = 'id'
    user = request.user
    user_dws = DwInfo.objects.filter(user=user)
    form = BaseFlowForm(user_dws=user_dws, col_names=col_names)
    display_tf = TableFactory(
        data_path=data_path,
        delimiter=delimiter,
        key=key
    )
    display_df = display_tf.create_df(nrows=100)
    desc = display_df.describe(datetime_is_numeric=True)
    if request.method == 'POST':
        form = BaseFlowForm(request.POST, user_dws=user_dws, col_names=col_names)
        if form.is_valid():
            dbname = request.POST['data_warehose']
            dw = DwInfo.objects.get(dbname__exact=dbname)
            connection_str = dw.get_conn_string()
            flow = BaseFlow(connection_str=connection_str, dimension_name=request.POST['dimension_name'])
            flow.save()
            conn_wrapper = flow.get_conn_wrapper()
            post = request.POST.copy()
            dimension_attributes = post.pop('dimension_attributes')
            lookupatts = post.pop('dimension_lookup_attributes')
            nrows = int(request.POST['nrows'])
            tf = TableFactory(
                dimension_attributes=dimension_attributes,
                data_path=data_path,
                delimiter=delimiter,
                key=key,
                flow=flow,
                conn_wrapper=conn_wrapper
            )
            df = tf.create_df(nrows=nrows)
            try:
                quoted_col_names = request.POST['quoted_col_names']
            except MultiValueDictKeyError:
                quoted_col_names = False
            try:
                create_table = request.POST['create_table']
            except MultiValueDictKeyError:
                create_table = False

            if create_table:
                tf = TableFactory(
                    data_path=data_path,
                    dimension_attributes=dimension_attributes,
                    delimiter=delimiter,
                    key=key,
                    flow=flow,
                    conn_wrapper=conn_wrapper
                )
                dimension_attributes = double_quote(dimension_attributes)
                lookupatts = double_quote(lookupatts)
                tf.create_table(df)

            if quoted_col_names:
                dimension_attributes = double_quote(dimension_attributes)
                df.columns = double_quote(df.columns)
                lookupatts = double_quote(lookupatts)

            pdsource = PandasSource(df)
            d = Dimension(
                name=flow.dimension_name,
                key=key,
                attributes=dimension_attributes,
                lookupatts=lookupatts
            )
            for row in pdsource:
                d.insert(row)
            file_handle.close()
            conn_wrapper.commit()
            conn_wrapper.close()
            return redirect('home')

    context = {
        'segment': 'etl',
        'form': form,
        'delimiter': delimiter,
        'csv_source': csv_source,
        'headers': col_names,
        'desc': desc
    }
    return render(request, 'etl_setup.html', context)


def descriptive_statistics(request):
    context = {
        'segment': 'etl',
    }
    return render(request, 'descriptive_statistics.html', context)


def etl(request):
    form = FileUploadForm()
    if request.method == 'POST':
        form = FileUploadForm(request.POST, request.FILES)
        if form.is_valid():
            data_source = request.FILES['data_source']
            delimiter = request.POST['delimiter']
            fs.save(data_source.name, data_source)
            request.session['delimiter'] = delimiter
            request.session['data_path'] = fs.path(data_source.name)
            form = FileUploadForm()
    context = {
        'segment': 'etl',
        'form': form
    }
    return render(request, 'etl.html', context)


def user_databases(request):
    dwinfo = DwInfo(user=request.user)
    form = DwInfoForm(instance=dwinfo)
    if request.method == 'POST':
        form = DwInfoForm(request.POST, instance=dwinfo)
        if form.is_valid():
            form.save()
            form = DwInfoForm()
    context = {
        'segment': 'user_databases',
        'form': form
    }
    return render(request, 'user_databases.html', context)
