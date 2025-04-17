/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.filters;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.TestModules;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectId;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

@Guice(modules = TestModules.TestOnlyModule.class)
public class CAFTests {
    public static final String                                                     ATLAS_REST_ADDRESS              = "atlas.rest.address";
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_STARTS_WITH_TEST = Triplet.with("name", AtlasRule.RuleExprObject.Operator.STARTS_WITH, "test");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_STARTS_WITH_DEMO = Triplet.with("name", AtlasRule.RuleExprObject.Operator.STARTS_WITH, "demo");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_QNAME_ENDS_WITH_TEST  = Triplet.with("qualifiedName", AtlasRule.RuleExprObject.Operator.ENDS_WITH, "test");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_CONTAINS_TEMP    = Triplet.with("name", AtlasRule.RuleExprObject.Operator.CONTAINS_IGNORECASE, "temp");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_DELETE_CLASSIFICATION = Triplet.with("operationType", AtlasRule.RuleExprObject.Operator.EQ, "CLASSIFICATION_DELETE");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_ENTITY_DELETED        = Triplet.with("operationType", AtlasRule.RuleExprObject.Operator.EQ, "ENTITY_DELETE");
    final     List<String>  allRuleGuids   = new ArrayList<>();
    final     List<String>  allEntityGuids = new ArrayList<>();
    protected AtlasClientV2 atlasClientV2;

    @BeforeClass
    public void setUp() throws Exception {
        Configuration configuration = ApplicationProperties.get();

        String[] atlasUrls = configuration.getStringArray(ATLAS_REST_ADDRESS);

        if (atlasUrls == null || atlasUrls.length == 0) {
            atlasUrls = new String[] {"http://localhost:21000/"};
        }
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClientV2 = new AtlasClientV2(atlasUrls, new String[] {"admin", "admin"});
        } else {
            atlasClientV2 = new AtlasClientV2(atlasUrls);
        }
        setupRules();
    }

    @Test
    public void testRule_1() throws AtlasServiceException {
        //Discard entity audits of hive_table and hive_db if name contains temp
        AtlasEntity tblEntity = createHiveTable("table_temp");
        assertNotNull(tblEntity);
        assertEntityAudits(tblEntity.getGuid(), 0); // discards ENTITY_CREATE audit event for hive_table as name contains "temp"

        AtlasEntity dbEntity = createDatabase("db_temp");
        assertNotNull(dbEntity);
        assertEntityAudits(dbEntity.getGuid(), 0); // discards ENTITY_CREATE audit event for hive_db as name contains "temp"

        AtlasEntity hdfsEntity = createHdfsPath("hdfspath_temp");
        assertNotNull(hdfsEntity);
        assertEntityAudits(hdfsEntity.getGuid(), 1); // accepts ENTITY_CREATE audit event even if name contains "temp"
    }

    @Test
    public void testRule_2() throws AtlasServiceException {
        //Discard entity audits for spark tables if the name starts with test or qualified name ends with test
        AtlasEntity atlasEntity = getBasicEntity("spark_table", "sptable_1");
        createEntity(atlasEntity);

        assertNotNull(atlasEntity);
        assertEntityAudits(atlasEntity.getGuid(), 1);

        // update this entity
        atlasEntity.setAttribute("name", "testPrefix_" + atlasEntity.getAttribute("name"));
        AtlasEntity.AtlasEntityWithExtInfo entity2info  = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        EntityMutationResponse             updateResult = atlasClientV2.updateEntity((entity2info));
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);
        assertEntityAudits(atlasEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, 0); // no audits for ENTITY_UPDATE event as name starts with test

        //update this entity - remove test prefix from name
        atlasEntity.setAttribute("name", "newPrefix_" + atlasEntity.getAttribute("name"));
        entity2info  = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        updateResult = atlasClientV2.updateEntity((entity2info));
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);
        assertEntityAudits(atlasEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, 1); // one ENTITY_UPDATE audit when name does not start with test

        //update this entity - add suffix to qualified name
        atlasEntity.setAttribute("qualifiedName", atlasEntity.getAttribute("qualifiedName") + "_test");
        entity2info  = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        updateResult = atlasClientV2.updateEntity((entity2info));
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);
        assertEntityAudits(atlasEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, 1); // no new audit; number of ENTITY_UPDATE audits did not change
    }

    @Test
    public void testRule_3() throws AtlasServiceException {
        //Discard entity audits for all types under "hive" hook
        AtlasEntity dbEntity = createDatabase("db01");
        assertNotNull(dbEntity);
        assertEntityAudits(dbEntity.getGuid(), 1);

        AtlasEntity tblEntity = createHiveTable("tbl01");
        assertNotNull(tblEntity);
        assertEntityAudits(tblEntity.getGuid(), 1);
        String clName = createClassification(tblEntity.getGuid(), "class01");
        assertEntityAudits(tblEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD, 1);
        try {
            Thread.sleep(1000); //hits error "ATLAS-406-00-001" otherwise
        } catch (InterruptedException ignored) {
        }
        removeEntityClassification(tblEntity, clName);
        assertEntityAudits(tblEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE, 0);

        AtlasEntity colEntity = getBasicEntity("hive_column", "col01");
        colEntity.setAttribute("type", "int");
        colEntity.setRelationshipAttribute("table", toAtlasRelatedObjectId(tblEntity));
        createEntity(colEntity);
        assertNotNull(colEntity);
        assertEntityAudits(colEntity.getGuid(), 1);

        clName = createClassification(colEntity.getGuid(), "class02");
        assertEntityAudits(colEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD, 1);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        removeEntityClassification(colEntity, clName);
        assertEntityAudits(colEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE, 0);

        EntityMutationResponse deleteResponse = atlasClientV2.deleteEntitiesByGuids(Collections.singletonList(colEntity.getGuid()));
        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertTrue(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size() > 0);
        assertEntityAudits(colEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, 0);
        allEntityGuids.remove(colEntity.getGuid());

        AtlasEntity hdfsEntity = createHdfsPath("hdfs01");
        assertNotNull(hdfsEntity);
        assertEntityAudits(hdfsEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, 1);
        clName = createClassification(hdfsEntity.getGuid(), "class03");
        assertEntityAudits(hdfsEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD, 1);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        removeEntityClassification(hdfsEntity, clName);
        assertEntityAudits(hdfsEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE, 1);
    }

    @Test
    public void testRule_4() throws AtlasServiceException {
        //Discard all entity audits for type Asset and its subtypes
        AtlasEntity tblEntity = createHiveTable("HiveAsset_tbl01");
        assertNotNull(tblEntity);
        assertEntityAudits(tblEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, 1);

        //update this entity
        tblEntity.setAttribute("name", "demoPrefix_" + tblEntity.getAttribute("name"));
        AtlasEntity.AtlasEntityWithExtInfo entity2info  = new AtlasEntity.AtlasEntityWithExtInfo(tblEntity);
        EntityMutationResponse             updateResult = atlasClientV2.updateEntity((entity2info));
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);
        assertEntityAudits(tblEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, 0); // No audit as name starts with demo

        AtlasEntity processEntity = getBasicEntity("Process", "demo_process_01");
        createEntity(processEntity);

        assertNotNull(processEntity);
        assertEntityAudits(processEntity.getGuid(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, 0);
    }

    public void removeEntityClassification(AtlasEntity entity, String classificationName) throws AtlasServiceException {
        Map<String, String> attrMap = new HashMap<String, String>() {
            {
                put("qualifiedName", (String) entity.getAttribute("qualifiedName"));
            }
        };
        atlasClientV2.removeClassification(entity.getTypeName(), attrMap, classificationName);
    }

    @AfterClass
    public void teardown() throws Exception {
        deleteAllRules();
        deleteAllEntities();
        AtlasGraphProvider.cleanup();
    }

    public void deleteAllRules() throws AtlasServiceException {
        for (String guid : allRuleGuids) {
            EntityMutationResponse resp = atlasClientV2.deleteRuleByGuid(guid);
            assertEquals(1, resp.getDeletedEntities().size());
        }
    }

    protected void createEntity(AtlasEntity atlasEntity) throws AtlasServiceException {
        EntityMutationResponse  entityMutationResponse = atlasClientV2.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity));
        List<AtlasEntityHeader> entitiesCreated        = entityMutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);
        if (CollectionUtils.isNotEmpty(entitiesCreated)) {
            String entityGuid = entitiesCreated.get(0).getGuid();
            allEntityGuids.add(entityGuid);
            atlasEntity.setGuid(entityGuid);
        }
    }

    protected String randomString() {
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(9);
    }

    private void setupRules() {
        try {
            //Create a rule to discard audits of event for a specific type based on attribute value (single condition)
            //CSV of type-names is supported
            createRule("rule_1", "hive_table,hive_db", false, CRITERION_NAME_CONTAINS_TEMP);

            //Create a rule to discard audits of event for a specific type based on attribute value (multiple conditions)
            createRule("rule_2", "spark_table", false, AtlasRule.Condition.OR, CRITERION_NAME_STARTS_WITH_TEST, CRITERION_QNAME_ENDS_WITH_TEST);

            //Create a rule to discard audits for all types under a hook type (using Regex support for wildcard character *)
            createRule("rule_3", "hive*", false, AtlasRule.Condition.OR, CRITERION_DELETE_CLASSIFICATION, CRITERION_ENTITY_DELETED);

            //Create a rule to discard all audits of a type and its subtypes
            createRule("rule_4", "Asset", true, CRITERION_NAME_STARTS_WITH_DEMO);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
            fail("SetupFailed with Exception");
        }
    }

    private String createClassification(String guid, String classificationName) throws AtlasServiceException {
        String                 clName = classificationName + randomString();
        AtlasClassificationDef clDef  = AtlasTypeUtil.createTraitTypeDef(clName, Collections.emptySet());
        atlasClientV2.createAtlasTypeDefs(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.singletonList(clDef), Collections.emptyList()));
        atlasClientV2.addClassifications(guid, Collections.singletonList(new AtlasClassification(clDef.getName())));
        return clName;
    }

    private void saveRuleGuid(String ruleName) throws AtlasServiceException {
        Map<String, String> attributes = Collections.singletonMap("ruleName", ruleName);
        AtlasEntity         ruleEntity = atlasClientV2.getEntityByAttribute("__AtlasRule", attributes).getEntity();
        allRuleGuids.add(ruleEntity.getGuid());
    }

    private void assertEntityAudits(String guid, int expected) throws AtlasServiceException {
        assertEntityAudits(guid, null, expected);
    }

    private void assertEntityAudits(String guid, EntityAuditEventV2.EntityAuditActionV2 auditAction, int expected) throws AtlasServiceException {
        List<EntityAuditEventV2> events = atlasClientV2.getAuditEvents(guid, "", auditAction, (short) 100);
        assertNotNull(events);
        assertEquals(events.size(), expected);
    }

    private AtlasRule.RuleExprObject getSimpleRuleExprObject(String typeName, boolean includeSubTypes, Triplet<String, AtlasRule.RuleExprObject.Operator, String> criterionTriplet) {
        return getSimpleRuleExprObject(typeName, includeSubTypes, criterionTriplet.getValue0(), criterionTriplet.getValue1(), criterionTriplet.getValue2());
    }

    private AtlasRule.RuleExprObject getSimpleRuleExprObject(String typeName, boolean includeSubTypes, String attributeName, AtlasRule.RuleExprObject.Operator operator, String attributeValue) {
        return new AtlasRule.RuleExprObject(typeName, attributeName, operator, attributeValue, includeSubTypes);
    }

    private AtlasRule.RuleExprObject getNestedRuleExprObject(String typeName, boolean includeSubTypes, Pair<AtlasRule.Condition, List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>>> conditionPair) {
        return getNestedRuleExprObject(typeName, includeSubTypes, conditionPair.getValue0(), conditionPair.getValue1());
    }

    private AtlasRule.RuleExprObject getNestedRuleExprObject(String typeName, boolean includeSubTypes, AtlasRule.Condition condition, List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>> criterion) {
        return new AtlasRule.RuleExprObject(typeName, condition, transform(criterion), includeSubTypes);
    }

    private List<AtlasRule.RuleExprObject.Criterion> transform(List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>> criterion) {
        return criterion.stream().map(criteriatriplet -> new AtlasRule.RuleExprObject.Criterion(criteriatriplet.getValue1(), criteriatriplet.getValue0(), criteriatriplet.getValue2())).collect(Collectors.toList());
    }

    @SafeVarargs
    private final void createRule(String ruleName,
            String typeName,
            boolean includeSubtypes,
            Triplet<String, AtlasRule.RuleExprObject.Operator, String>... criteria) throws AtlasServiceException {
        createRule(ruleName, typeName, includeSubtypes, null, criteria);
    }

    @SafeVarargs
    private final void createRule(String ruleName,
            String typeName,
            boolean includeSubtypes,
            AtlasRule.Condition condition, Triplet<String, AtlasRule.RuleExprObject.Operator, String>... criteria) throws AtlasServiceException {
        AtlasRule.RuleExpr ruleExpr = null;

        if (condition == null || (criteria != null && criteria.length <= 1)) {
            condition = null;
            AtlasRule.RuleExprObject simpleRuleExprObj = getSimpleRuleExprObject(typeName, includeSubtypes, criteria[0]);
            ruleExpr = new AtlasRule.RuleExpr(Collections.singletonList(simpleRuleExprObj));
        }

        if (condition != null && criteria != null) {
            List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>>                            criteriaList      = Arrays.asList(criteria);
            Pair<AtlasRule.Condition, List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>>> condition1        = Pair.with(condition, criteriaList);
            AtlasRule.RuleExprObject                                                                    nestedRuleExprObj = getNestedRuleExprObject(typeName, includeSubtypes, condition1);
            ruleExpr = new AtlasRule.RuleExpr(Collections.singletonList(nestedRuleExprObj));
        }

        AtlasRule atlasRule = new AtlasRule();
        atlasRule.setAction("DISCARD");
        atlasRule.setRuleName(ruleName);
        atlasRule.setRuleExpr(ruleExpr);

        AtlasRule rule = atlasClientV2.createRule(atlasRule);
        saveRuleGuid(rule.getRuleName());
    }

    private AtlasEntity createDatabase(String name) throws AtlasServiceException {
        AtlasEntity atlasEntity = getBasicEntity("hive_db", name);
        atlasEntity.setAttribute("clusterName", "cl1");
        createEntity(atlasEntity);
        return atlasEntity;
    }

    private AtlasEntity createHiveTable(String name) throws AtlasServiceException {
        AtlasEntity atlasEntity = getBasicEntity("hive_table", name);
        createEntity(atlasEntity);
        return atlasEntity;
    }

    private AtlasEntity createHdfsPath(String name) throws AtlasServiceException {
        AtlasEntity atlasEntity = getBasicEntity("hdfs_path", name);
        atlasEntity.setAttribute("path", atlasEntity.getAttribute("name") + "_path");
        createEntity(atlasEntity);
        return atlasEntity;
    }

    private AtlasEntity getBasicEntity(String typeName, String name) {
        AtlasEntity atlasEntity = new AtlasEntity(typeName);
        atlasEntity.setAttribute("name", name + "" + randomString());
        atlasEntity.setAttribute("qualifiedName", "q" + atlasEntity.getAttribute("name"));
        return atlasEntity;
    }

    private void deleteAllEntities() throws AtlasServiceException {
        EntityMutationResponse deleteResponse = atlasClientV2.deleteEntitiesByGuids(allEntityGuids);

        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), allEntityGuids.size());
    }
}
