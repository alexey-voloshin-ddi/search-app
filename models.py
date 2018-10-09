from django.db import models


class EmergencyModal(models.Model):
    active = models.BooleanField()
    heading_text = models.CharField(max_length=48, help_text="Up to 48 characters")
    message_text = models.TextField()
    button_text = models.CharField(max_length=48, help_text="Up to 48 characters")


class LatestSaving(models.Model):
    position = models.IntegerField()
    place_name = models.CharField(max_length=48)
    check_in = models.DateField()
    night_count = models.IntegerField()
    currency_symbol = models.CharField(max_length=1)
    absolute_saving = models.FloatField()
    percentage_saving = models.FloatField()

    class Meta:
        ordering = ['position']
