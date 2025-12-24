from django.shortcuts import render

# Create your views here.
def health_check(request):
    return render(request, "main/health_check.html")