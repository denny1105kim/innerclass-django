from django.db import migrations
from pgvector.django import VectorExtension

# Monkey patch or just add hints to avoid AttributeError
# This is a workaround for some versions
try:
    VectorExtension.hints = {}
except:
    pass

class Migration(migrations.Migration):

    dependencies = []

    operations = [
        VectorExtension()
    ]
