<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/bahmnisyncmaster/config.form" />

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="template/localHeader.jsp" %>

<style type="text/css">
.settingRow {
	padding-top: 1em;
	clear: both;
}
.settingName {
	padding: 0.25em;
	background-color: #e0e0e0;
}
.settingValue {
	float: left;
	width: 40%;
}
.settingDescription {
	font-size: 0.8em;
	float: left;
	width: 55%;
}
.saveButtons {
	padding-left: 0.5em;
	background-color: #e0e0e0;
	clear: both;
}
</style>

<h2>Master Configuration</h2>

<spring:hasBindErrors name="globalPropertiesModel">
	<spring:message code="fix.error"/>
</spring:hasBindErrors>

<form:form method="post" modelAttribute="globalPropertiesModel">
	<c:forEach var="prop" items="${globalPropertiesModel.properties}" varStatus="varStatus">
		<spring:nestedPath path="properties[${varStatus.index}]">
			<div class="settingRow">
				<h4 class="settingName"><%-- <spring:message code="${prop.property}.label" /> --%>

				<c:if test="${prop.property == 'bahmnisyncmaster.kafka.url' }">
					KAFKA URL
				</c:if>
				<c:if test="${prop.property == 'bahmnisyncmaster.database.server.name' }">
					Database Server Name
				</c:if>
				<c:if test="${prop.property == 'bahmnisyncmaster.openmrs.schema.name' }">
					OpenMRS Schema Name
				</c:if>
				<c:if test="${prop.property == 'bahmnisyncmaster.conflict.resolution.rule' }">
					Conflict Resolution Rule
				</c:if>
				
				</h4>
				<span class="settingValue">
					<spring:bind path="propertyValue">
						<c:set var="inputSize" value="50" scope="page" />
						<input type="text" name="${status.expression}" value="${status.value}" size="${inputSize}">
						<form:errors cssClass="error"/>
					</spring:bind>
				</span>
				<span class="settingDescription">
					${prop.description}
				</span>
			</div>
		</spring:nestedPath>
	</c:forEach>

	<div class="settingRow">
		<div class="saveButtons">
			<input type="submit" value='<spring:message code="general.save"/>' /> &nbsp;&nbsp; 
			<input type="button" value='<spring:message code="general.cancel"/>' onclick="javascript:window.location='<openmrs:contextPath />/admin'" />
		</div>
	</div>
</form:form>

<%@ include file="/WEB-INF/template/footer.jsp" %>
