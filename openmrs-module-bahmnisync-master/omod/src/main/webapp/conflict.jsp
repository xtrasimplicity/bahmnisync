<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/webservices/rest/settings.form" />

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="template/localHeader.jsp" %>

<h2><spring:message code="bahmnisyncmaster.sync.conflict" /></h2>

<table>
 <thead>
  <tr>
	  <th>Date of Sync</th>
	  <th>Worker Node ID </th>
	  <th>Change from Worker</th>
	  <th>Master Data</th> 
	  <th>Result</th>
  </tr>
 </thead>
 <tbody>
  	<c:forEach var="con" items="${mastersyncconflicts}" varStatus="varStatus">
		<spring:nestedPath path="properties[${varStatus.index}]">
		<tr>
		<td>${con.logDateTime}</td>
		<td>${con.workerId}</td>
		<td>${con.workerData}</td>
		<td>${con.masterData}</td>
		<td>${con.message}</td>
		</tr>
		</spring:nestedPath>
	</c:forEach>	  
  </tbody>
</table>	        

<%@ include file="/WEB-INF/template/footer.jsp" %>
