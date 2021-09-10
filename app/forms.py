from django import forms
from django.forms import ModelForm
from .models import DwInfo


class BaseFlowForm(forms.Form):
    def __init__(self, *args, **kwargs):
        col_names = kwargs.pop('col_names', None)
        user_dws = kwargs.pop('user_dws', None)
        super(BaseFlowForm, self).__init__(*args, **kwargs)
        self.fields['dimension_attributes'].choices = [(n, n) for n in col_names]
        self.fields['dimension_lookup_attributes'].choices = [(n, n) for n in col_names]
        self.fields['data_warehose'].choices = [(dw, dw) for dw in user_dws]

    data_warehose = forms.ChoiceField(
        required=True,
        choices=(),
        widget=forms.Select(
            attrs={
                'class': 'form-control form-control',
                'id': 'defaultSelect'
            }
        )
    )
    dimension_name = forms.CharField(
        required=True,
        widget=forms.TextInput(
            attrs={
            }
        )
    )
    dimension_attributes = forms.MultipleChoiceField(
        required=True,
        choices=(),
        widget=forms.SelectMultiple(
            attrs={
                'class': 'form-control'
            }
        )
    )

    dimension_lookup_attributes = forms.MultipleChoiceField(
        required=True,
        choices=(),
        widget=forms.SelectMultiple(
            attrs={
                'class': 'form-control'
            }
        )
    )

    quoted_col_names = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(
            attrs={
                'class': 'form-check-input'
            }
        )
    )

    create_table = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(
            attrs={
                'class': 'form-check-input'
            }
        )
    )


class FileUploadForm(forms.Form):
    data_source = forms.FileField(
        required=True,
        widget=forms.FileInput(
            attrs={
                'type': 'file',
                'class': 'form-control-file',
                'id': 'exampleFormControlFile1',
            }
        )
    )

    delimiter = forms.ChoiceField(
        required=True,
        choices=[(';', ';'), (',', ',')],
        widget=forms.Select(attrs={
            'class': 'form-control',
            'id': 'formGroupDefaultSelect'
        }
        )
    )


class DwInfoForm(ModelForm):
    class Meta:
        model = DwInfo
        fields = ['host', 'dbname', 'db_user', 'db_password', 'db_port']
