# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import asyncio
import csv
import io
import os
import pandas as pd
import json


import pygrametl
import psycopg2
from pygrametl.datasources import CSVSource, MergeJoiningSource, TransformingSource
from pygrametl.tables import Dimension
from time import sleep
from django_globals import globals
from django.core.files.storage import FileSystemStorage
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.template import loader
from django.http import HttpResponse, HttpResponseRedirect, StreamingHttpResponse
from django import template
from .forms import BaseFlowForm, FileUploadForm
from .models import BaseFlow


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


fs = FileSystemStorage(location='app/uploads')
upload_dir = fs.location


@login_required(login_url="/login/")
def etl_setup(request):
    delimiter = request.session['delimiter']
    data_path = request.session['data_path']
    file_handle = io.open(data_path, 'r', 16394, encoding='utf-8-sig')
    csv_source = CSVSource(file_handle, delimiter=delimiter)
    col_names = list(csv_source.fieldnames)
    form = BaseFlowForm(col_names)

    if request.method == 'POST':
        form = BaseFlowForm(request.POST, request.FILES)
        if form.is_valid():
            flow = BaseFlow(**form.cleaned_data)
            flow.save()
            dimension_attributes = request.POST['dimension_attributes']
            lookupattrs = request.POST['dimension_lookup_attributes']
            conn_wrapper = flow.get_conn_wrapper()
            d = Dimension(
                name=flow.dimension_name,
                key='id',
                attributes=dimension_attributes,
                lookupatts=lookupattrs
            )
            for row in csv_source:
                d.insert(row)
            file_handle.close()
            conn_wrapper.commit()
            conn_wrapper.close()

        form = BaseFlowForm()

    context = {
        'segment': 'etl',
        'form': form,
        'delimiter': delimiter,
        'csv_source': csv_source,
        'headers': col_names
    }
    return render(request, 'etl_setup.html', context)


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

