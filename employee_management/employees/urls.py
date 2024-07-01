from django.urls import path
from .views import EmployeeListCreate, EmployeeRetrieveUpdateDestroy

urlpatterns = [
    path('', EmployeeListCreate.as_view(), name='employee-list-create'),
    path('<int:pk>/', EmployeeRetrieveUpdateDestroy.as_view(), name='employee-retrieve-update-destroy'),
]