<%@ include file="/WEB-INF/template/include.jsp"%>

<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.12/css/dataTables.bootstrap.css" />
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/colreorder/1.3.2/css/colReorder.bootstrap.css" />
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/buttons/1.2.2/css/buttons.dataTables.min.css" />

<script type="text/javascript" src="https://code.jquery.com/jquery-3.1.0.js"></script>
<script type="text/javascript" src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js" integrity="sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa" crossorigin="anonymous"></script>
<script type="text/javascript" src="https://cdn.datatables.net/1.10.12/js/jquery.dataTables.min.js"></script>
<script type="text/javascript" src="https://cdn.datatables.net/1.10.12/js/dataTables.bootstrap.js"></script>
<script type="text/javascript" src="https://cdn.datatables.net/colreorder/1.3.2/js/dataTables.colReorder.js"></script>
<script type="text/javascript" src="https://cdn.datatables.net/buttons/1.2.2/js/dataTables.buttons.min.js"></script>
<script type="text/javascript" src="https://cdn.datatables.net/buttons/1.2.2/js/buttons.html5.min.js"></script>
<script type="text/javascript" src="https://cdn.datatables.net/buttons/1.2.2/js/buttons.print.min.js"></script>


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

</br>

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
	  <th>Entity Identifier</th>
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
		<td>${con.status}</td>
		</tr>
		</spring:nestedPath>
	</c:forEach>	  
  </tbody>
</table>	        

<%@ include file="/WEB-INF/template/footer.jsp" %>
