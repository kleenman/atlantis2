from django import forms
from .models import BaseFlow


class BaseFlowForm(forms.Form):
    def __init__(self, col_names, *args, **kwargs):
        super(BaseFlowForm, self).__init__(*args, **kwargs)
        self.fields['dimension_attributes'].choices = [(n, n) for n in col_names]
        self.fields['dimension_lookup_attributes'].choices = [(n, n) for n in col_names]

    connection_str = forms.CharField(
        required=True
    )
    dimension_name = forms.CharField(
        required=True,
        widget=forms.TextInput(
            attrs={
            }
        )
    )
    dimension_attributes = forms.ChoiceField(
        required=True,
        choices=(),
        widget=forms.SelectMultiple(
            attrs={
                'class': 'form-control'
            }
        )
    )

    dimension_lookup_attributes = forms.ChoiceField(
        required=True,
        choices=(),
        widget=forms.SelectMultiple(
            attrs={
                'class': 'form-control'
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


