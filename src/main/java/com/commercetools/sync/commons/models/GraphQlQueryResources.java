package com.commercetools.sync.commons.models;

public enum GraphQlQueryResources {
    CATEGORIES("categories"),
    CHANNELS("channels"),
    CUSTOMER_GROUPS("customerGroups"),
    CUSTOMERS("customers"),
    PRODUCTS("products"),
    PRODUCT_TYPES("productTypes"),
    STATES("states"),
    TAX_CATEGORIES("taxCategories"),
    TYPES("types"),
    SHOPPING_LISTS("shoppingLists");

    private final String name;

    GraphQlQueryResources(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}