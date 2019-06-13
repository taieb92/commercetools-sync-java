package com.commercetools.sync.producttypes.utils;

import com.commercetools.sync.commons.exceptions.InvalidAttributeDefinitionException;
import com.commercetools.sync.commons.exceptions.InvalidProductTypeException;
import com.commercetools.sync.commons.exceptions.InvalidReferenceException;
import com.commercetools.sync.commons.exceptions.ReferenceReplacementException;
import io.sphere.sdk.expansion.ExpansionPath;
import io.sphere.sdk.models.Reference;
import io.sphere.sdk.products.attributes.AttributeDefinitionDraft;
import io.sphere.sdk.products.attributes.AttributeDefinitionDraftBuilder;
import io.sphere.sdk.products.attributes.AttributeType;
import io.sphere.sdk.products.attributes.NestedAttributeType;
import io.sphere.sdk.products.attributes.SetAttributeType;
import io.sphere.sdk.producttypes.ProductType;
import io.sphere.sdk.producttypes.ProductTypeDraft;
import io.sphere.sdk.producttypes.ProductTypeDraftBuilder;
import io.sphere.sdk.producttypes.queries.ProductTypeQuery;
import io.sphere.sdk.queries.QueryExecutionUtils;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ProductTypeReferenceReplacementUtils {

    /**
     * Takes a list of ProductTypes that are supposed to have their productType references (in case it has nestedType or
     * set of NestedType) references expanded in order to be able to fetch the keys and replace the reference ids with
     * the corresponding keys and then return a new list of productType drafts with their references containing keys
     * instead of the ids.
     *
     * <p><b>Note:</b>If some references are not expanded for an attributeDefinition of a productType, the method will
     * throw a {@link ReferenceReplacementException} containing the root causes of the exceptions that occurred in any
     * of the supplied {@code productTypes}.
     *
     * @param productTypes the list of productTypes to to replace the references on and convert to productTypeDrafts.
     * @return a list of productType drafts with keys instead of ids for references.
     */
    @Nonnull
    public static List<ProductTypeDraft> replaceProductTypesReferenceIdsWithKeys(
        @Nonnull final List<ProductType> productTypes) {

        final Set<Throwable> potentialErrors = new HashSet<>();

        final List<ProductTypeDraft> referenceReplacedDrafts = productTypes
            .stream()
            .filter(Objects::nonNull)
            .map(productType -> {
                final ProductTypeDraft productTypeDraft = ProductTypeDraftBuilder.of(productType)
                                                                                 .build();

                List<AttributeDefinitionDraft> referenceReplacedAttributeDefinitions;
                try {
                    referenceReplacedAttributeDefinitions =
                        replaceAttributeDefinitionsReferenceIdsWithKeys(productType);
                } catch (InvalidProductTypeException invalidProductTypeException) {
                    potentialErrors.add(invalidProductTypeException);
                    return null;
                }

                return ProductTypeDraftBuilder.of(productTypeDraft)
                                              .attributes(referenceReplacedAttributeDefinitions)
                                              .build();
            })
            .filter(Objects::nonNull)
            .collect(toList());

        if (!potentialErrors.isEmpty()) {
            throw new ReferenceReplacementException("Some errors occurred during reference replacement.",
                potentialErrors);
        }

        return referenceReplacedDrafts;
    }

    @Nonnull
    private static List<AttributeDefinitionDraft> replaceAttributeDefinitionsReferenceIdsWithKeys(
        @Nonnull final ProductType productType) throws InvalidProductTypeException {

        final Set<Throwable> potentialErrors = new HashSet<>();

        final List<AttributeDefinitionDraft> refReplacedDrafts = productType
            .getAttributes()
            .stream()
            .map(attributeDefinition -> {
                final AttributeType attributeType = attributeDefinition.getAttributeType();
                try {
                    final AttributeType referenceReplacedType = replaceProductTypeReferenceIdWithKey(attributeType);
                    return AttributeDefinitionDraftBuilder
                        .of(attributeDefinition)
                        .attributeType(referenceReplacedType)
                        .build();
                } catch (InvalidReferenceException exception) {
                    final InvalidAttributeDefinitionException attributeDefinitionException =
                        new InvalidAttributeDefinitionException(format(
                            "Failed to replace some references on the attributeDefinition with name '%s'. Cause: %s",
                            attributeDefinition.getName(),
                            exception.getMessage()),
                            exception);
                    potentialErrors.add(attributeDefinitionException);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(toList());

        if (!potentialErrors.isEmpty()) {
            throw new InvalidProductTypeException(
                format("Failed to replace some references on the productType with key '%s'.", productType.getKey()),
                potentialErrors);
        }
        return refReplacedDrafts;
    }

    @Nonnull
    private static AttributeType replaceProductTypeReferenceIdWithKey(
        @Nonnull final AttributeType attributeType) throws InvalidReferenceException {

        if (attributeType instanceof NestedAttributeType) {
            final Reference<ProductType> referenceReplacedNestedType =
                replaceProductTypeReferenceIdWithKey((NestedAttributeType) attributeType);

            return NestedAttributeType.of(referenceReplacedNestedType);
        } else {
            if (attributeType instanceof SetAttributeType) {
                final SetAttributeType setAttributeType = (SetAttributeType) attributeType;
                final AttributeType elementType = setAttributeType.getElementType();
                final AttributeType referenceReplacedElementType = replaceProductTypeReferenceIdWithKey(elementType);
                return SetAttributeType.of(referenceReplacedElementType);
            }
        }
        return attributeType;
    }

    @Nonnull
    private static Reference<ProductType> replaceProductTypeReferenceIdWithKey(
        @Nonnull final NestedAttributeType nestedAttributeType) throws InvalidReferenceException {

        final Reference<ProductType> productTypeReference = nestedAttributeType.getTypeReference();

        final ProductType productTypeReferenceObj = productTypeReference.getObj();
        if (productTypeReferenceObj != null) {
            return ProductType.referenceOfId(productTypeReferenceObj.getKey());
        } else {
            throw new InvalidReferenceException("ProductType reference is not expanded.");
        }
    }

    /**
     * Builds a {@link ProductTypeQuery} for fetching products from a source CTP project with an expansion to the
     * {@link ProductType} reference of any
     * <a href="https://docs.commercetools.com/http-api-projects-productTypes#nestedtype">NestedType Attribute</a>
     *
     * <p>Note: Please only use this util if you have nestedType attributes in the productTypes that you will fetch from
     * a source commercetools project. Otherwise, it is more efficient to build the query without expansions, if they
     * are not needed, to avoid unnecessarily bigger payloads fetched from the source project.
     *
     * @return the query for fetching products from the source CTP project with an expansion to the {@link ProductType}
     *         reference of any NestedType Attribute.
     */
    @Nonnull
    public static ProductTypeQuery buildProductTypeQuery() {
        return buildProductTypeQuery(0);
    }

    /**
     * Builds a {@link ProductTypeQuery} for fetching products from a source CTP project with an expansion to the
     * {@link ProductType} reference of any
     * <a href="https://docs.commercetools.com/http-api-projects-productTypes#nestedtype">NestedType Attribute</a> and
     * also any <a href="https://docs.commercetools.com/http-api-projects-productTypes#settype">SetType Attribute</a>
     * that has an {@code elementType} of {@code NestedType} depending on the given {@code maximumSetDepth} will provide
     * an expansion for the nestedType of this set depth and all nested types of smaller depths. For example:
     * <br><br>
     *     if {@code maximumSetDepth} is 0, it means there is no atttribute that is a type of set of nestedType.
     *     Therefore, the built query will have the following expansion paths:<br>
     *     attributes[*].type.typeReference
     * <br><br>
     *      if {@code maximumSetDepth} is 1, it means the maximum nesting of sets is set of a NestedType.
     *      Therefore, the built query will have the following expansion paths:<br>
     *      attributes[*].type.typeReference, attributes[*].type.elementType.typeReference
     * <br><br>
     *     if {@code maximumSetDepth} is 2, it means the maximum nesting of sets is set of set of a NestedType.
     *     Therefore, the built query will have the following expansion paths:<br>
     *     attributes[*].type.typeReference, attributes[*].type.elementType.typeReference,
     *     attributes[*].type.elementType.elementType.typeReference
     *<br><br>
     *     and so on..
     *
     * <p>Note: Please only use this util if you have nestedType attributes in the productTypes that you will fetch from
     *    a source commercetools project. Otherwise, it is more efficient to build the query without expansions, if they
     *    are not needed, to avoid unnecessarily bigger payloads fetched from the source project.
     *
     * @return the query for fetching productTypes from the source CTP project with all the aforementioned references
     *         expanded.
     */
    @Nonnull
    public static ProductTypeQuery buildProductTypeQuery(final int maximumSetDepth) {
        final String nestedTypeReferenceExpansionPath = "attributes[*].type.typeReference";
        final ProductTypeQuery productTypeQuery = ProductTypeQuery
            .of()
            .withLimit(QueryExecutionUtils.DEFAULT_PAGE_SIZE)
            .plusExpansionPaths(ExpansionPath.of(nestedTypeReferenceExpansionPath));


        final List<ExpansionPath<ProductType>> setsExpansionPath =
            buildSetOfNestedTypeReferenceExpansionPath(maximumSetDepth);

        return setsExpansionPath.isEmpty()
            ? productTypeQuery : productTypeQuery.plusExpansionPaths(setsExpansionPath);
    }

    @Nonnull
    private static List<ExpansionPath<ProductType>>
    buildSetOfNestedTypeReferenceExpansionPath(final int maximumSetDepth) {

        return IntStream.rangeClosed(1, maximumSetDepth)
                        .mapToObj(ProductTypeReferenceReplacementUtils::getExpansionPathForSetDept)
                        .collect(toList());
    }

    private static ExpansionPath<ProductType> getExpansionPathForSetDept(final int maximumSetDepth) {

        final String elementTypePath = IntStream.range(0, maximumSetDepth)
                                                .mapToObj(index -> "elementType")
                                                .collect(Collectors.joining("."));
        return ExpansionPath.of(format("attributes[*].type.%s.typeReference", elementTypePath));
    }

    private ProductTypeReferenceReplacementUtils() {
    }
}
