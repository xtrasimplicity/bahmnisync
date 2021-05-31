<%@ include file="/WEB-INF/template/include.jsp"%>
<%@ include file="/WEB-INF/template/header.jsp"%>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/bahmnisyncmaster/errors.form" />
</br>
<%@ include file="template/localHeader.jsp" %>

<html>

<head>

    <link rel="stylesheet" type="text/css" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
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
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.3/jszip.min.js"></script>
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/pdfmake.min.js"></script>
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/vfs_fonts.js"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/buttons/1.7.0/js/dataTables.buttons.min.js"></script>

</head>

<style>

h2 {
    display: block;
    font-size: 1.5em;
    margin-block-start: 0.83em;
    margin-block-end: 0.83em;
    margin-inline-start: 0px;
    margin-inline-end: 0px;
    font-weight: bold;
}

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
  background-color: #CBC3E3;
}

</style> 


<body>

<h2>Sync Errors</h2>

<table id="example">
 
 
 
<thead>
 
 
 
<tr>
 

	  <th>Date of Sync</th>
	  <th>Worker Node ID </th>
	  <th>Table</th>
	  <th>Error</th> 
	  <th>Data</th> 
 
</tr>
 
 
 
</thead>
 
 
 
</table>

</body>

<script type="text/javascript">

$(document).ready(function(){
	
	
	function formatJSONDate(dateInput, type) {
	   if (dateInput === null) {
	     return '';
	   }
	   if (type === 'display') {
		   
		 var date = new Date(dateInput);  
		 
		 var year = date.getFullYear();

		  var month = (1 + date.getMonth()).toString();
		  month = month.length > 1 ? month : '0' + month;

		  var day = date.getDate().toString();
		  day = day.length > 1 ? day : '0' + day;
		  
		  var hours = date.getHours();
		  //hours = hours.length > 1 ? hours : '0' + hours;
		  
		  var minutes = date.getMinutes();
		  //minutes = minutes.length > 1 ? minutes : '0' + minutes;
		  
		  var seconds = date.getSeconds();
		  //seconds = seconds.length > 1 ? seconds : '0' + seconds;
		  
		  return year + '-' + month + '-' + day + " " + hours + ":" + minutes + ":" + seconds;
	   }
	   return dateInput;
	 }	
	
	var data =eval('${mastersyncerrors}');

	var table = $('#example').DataTable( {
	"aaData": data,
	"aoColumns": [
			{ "mData": "logDateTime", "mRender": function(data, type, full) {
		        return formatJSONDate(data, 'display');
		    }},
			{ "mData": "workerId"},
			{ "mData": "table"},
			{ "mData": "message"},
			{ "mData": "workerData"}
		],
	 "dom": 'Bfrtip',
	 "buttons": [
		 {"extend": "copyHtml5", filename: function () {
			 var d = new Date();
			 var strDate = d.getFullYear() + "-" + (d.getMonth()+1) + "-" + d.getDate();
	         return strDate + ' - Sync Errors';
	      }},
		 {"extend": "excelHtml5", filename: function () {
			 var d = new Date();
			 var strDate = d.getFullYear() + "-" + (d.getMonth()+1) + "-" + d.getDate();
	         return strDate + ' - Sync Errors';
	      }},
		 {"extend": "csvHtml5", filename: function () {
			 var d = new Date();
			 var strDate = d.getFullYear() + "=" + (d.getMonth()+1) + "-" + d.getDate();
	         return strDate + ' - Sync Errors';
	      }},
		 {"extend": "pdfHtml5", filename: function () {
			 var d = new Date();
			 var strDate = d.getFullYear() + "-" + (d.getMonth()+1) + "-" + d.getDate();
	         return strDate + ' - Sync Errors';
	      }}
       ]
		});
	});
</script>

</html>

<%@ include file="/WEB-INF/template/footer.jsp" %>

