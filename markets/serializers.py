from rest_framework import serializers

class MarketSessionSerializer(serializers.Serializer):
    status = serializers.CharField()
    asof = serializers.DateTimeField()
    calendar_code = serializers.CharField()
    reason = serializers.CharField()
    next_open_at = serializers.DateTimeField(allow_null=True, required=False)
    prev_close_at = serializers.DateTimeField(allow_null=True, required=False)
