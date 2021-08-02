# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path
from app import views

urlpatterns = [

    # The home page
    path('', views.home, name='home'),

    path('etl/', views.etl, name='etl'),

    path('etl_setup/', views.etl_setup, name='etl_setup'),

    # Matches any html file
    re_path(r'^.*\.*', views.pages, name='pages')

]
