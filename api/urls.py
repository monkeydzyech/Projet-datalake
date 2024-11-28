from django.urls import path
from .views import (
    TotalSalesPerProductView,
    StatusCodeCountsView,
    AvgLikesPerProductView,
    CampaignMetricsView,
)

urlpatterns = [
    path("total-sales-per-product/", TotalSalesPerProductView.as_view(), name="total_sales_per_product"),
    path("status-code-counts/", StatusCodeCountsView.as_view(), name="status_code_counts"),
    path("avg-likes-per-product/", AvgLikesPerProductView.as_view(), name="avg_likes_per_product"),
    path("campaign-metrics/", CampaignMetricsView.as_view(), name="campaign_metrics"),
]
