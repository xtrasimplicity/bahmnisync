<%@ include file="/WEB-INF/template/include.jsp"%>

<style>
table {
  font-family: arial, sans-serif;
  border-collapse: collapse;
  width: 100%;
}

td, th {
  border: 1px solid #dddddd;
  text-align: left;
  padding: 8px;
}

tr:nth-child(even) {
  background-color: #dddddd;
}
</style>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/bahmnisyncmaster/conflict.form" />

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="template/localHeader.jsp" %>

<h2>Sync Conflicts</h2>

<table>
 <thead>
  <tr>
	  <th>Date of Sync</th>
	  <th>Worker Node ID </th>
	  <th>Table</th>
	  <th>
	  	 Conflicts <br/> 
	  	 <i> Column Name: Master Value, Worker Value </i>
	  </th> 
	  <th>Result</th>
  </tr>
 </thead>
 <tbody>
  	<c:forEach var="con" items="${mastersyncconflicts}" varStatus="varStatus">
		<spring:nestedPath path="properties[${varStatus.index}]">
		<tr>
		<td>${con.logDateTime}</td>
		<td>${con.workerId}</td>
		<td>${con.table}</td>
		<td>${con.masterData}</td>
		<td>${con.message}</td>
		</tr>
		</spring:nestedPath>
	</c:forEach>	  
  </tbody>
</table>	        

<%@ include file="/WEB-INF/template/footer.jsp" %>
