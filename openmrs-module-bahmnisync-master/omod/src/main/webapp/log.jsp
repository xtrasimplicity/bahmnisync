<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/webservices/rest/settings.form" />

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="template/localHeader.jsp" %>

<h2><spring:message code="bahmnisyncmaster.sync.log" /></h2>

<table>
 <thead>
  <tr>
	  <th>Date of Sync</th>
	  <th>Worker Node ID </th>
	  <th>Table</th>
	  <th>Change from Worker</th>
  </tr>
 </thead>
 <tbody>
  	<c:forEach var="log" items="${mastersynclogs}" varStatus="varStatus">
		<spring:nestedPath path="properties[${varStatus.index}]">
		<tr>
		<td>${log.logDateTime}</td>
		<td>${log.workerId}</td>
		<td>${log.table}</td>
		<td>${log.workerData}</td>
		</tr>
		</spring:nestedPath>
	</c:forEach>	  
  </tbody>
</table>	        

<%@ include file="/WEB-INF/template/footer.jsp" %>
