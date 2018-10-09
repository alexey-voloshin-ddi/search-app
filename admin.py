from django.contrib import admin

from apps.search.models import EmergencyModal, LatestSaving


admin.site.register(EmergencyModal)
admin.site.register(LatestSaving)
