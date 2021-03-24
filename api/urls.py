from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from .views import manage_items, manage_item

urlpatterns = {
    path('', manage_items, name="items"),
    path('<slug:key>', manage_item, name="single_item")
    #path('ddl/<slug:query>', manage_item),
    #path('pca/<slug:query>', manage_item)
}
urlpatterns = format_suffix_patterns(urlpatterns)
