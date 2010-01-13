from django.template.loader import render_to_string

def render_template(path, context=dict()):
  return render_to_string(path, context)
