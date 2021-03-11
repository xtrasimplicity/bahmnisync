<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Manage Bahmni Sync" otherwise="/login.htm" redirect="/module/bahmnisyncworker/sync.form" />

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="template/localHeader.jsp" %>

<script type="text/javascript">

    function inProgress() {
        $j("#syncButton").prop("disabled", true);
        $j("#failure").hide();
        $j("#success").hide();
        $j("#progress").show();
    }
    function onSuccess(data) {
        $j("#progress").hide();
        if (data && data.success) {
            $j("#success").show();
        } else {
            $j("#failure").show();
        }
        $j("#syncButton").prop("disabled", false);
    }
    function onError(data) {
        $j("#progress").hide();
        $j("#success").hide();
        $j("#failure").show();
        $j("#syncButton").prop("disabled", false);
        addLog('Error: Process stopped!! ');
    }
    function syncProcess() {
    	
    	$j("#progress").hide();
        $j("#success").hide();
        $j("#failure").hide();
    	$j('#log').val('');
    	
    	isSyncReady();
    	
    }
    function isSyncReady(){
		addLog('Checking Bahmnisync global properties...');
    	
    	$j.ajax({
    		"type": "GET",
            "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/syncready.form",
            "data": {},
            "dataType": "json",
            "success": checkConnection,
            "error": onError
        });
    }
    function checkConnection(data){
    	if (data.ready === "yes") {
    		addLog('Global Properties are available...');
    		
    		addLog('Checking Connection to Master...');
    		$j.ajax({
                "type": "GET",
                "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/checkconnection.form",
                "data": {},
                "dataType": "json",
                "success": pushData,
                "error": onError
            });
        } else{
        	addLog('Some or all Global Properties missing...');
        	addLog('Process stopped!!');
        }
    } 
	function pushData(data){
		if (data.ready === "yes") {
			addLog('Connection established to Master...');
			
			addLog('Starting data push to Master...');
    		$j.ajax({
                "type": "GET",
                "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/startPushData.form",
                "data": {},
                "dataType": "json",
                "success": pullData,
                "error": onError
            });
		} else{
			addLog('Error while pushing data...');
        	addLog('Process stopped!!');
		}
    }
	function pullData(data){
		if (data.ready === "yes") {
			addLog('Data Push completed...');
			
			addLog('Starting data pull from Master...');
			$j.ajax({
                "type": "GET",
                "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/startPullData.form",
                "data": {},
                "dataType": "json",
                "success": complete,
                "error": onError
            });
		} else{
			addLog('Error while pushing data...');
        	addLog('Process stopped!!');
		}
    }
	function complete(data){
		if (data.ready === "yes") {
			addLog('Data Pull completed...');
			addLog('Process completed!!');
			$j("#success").show();
		} else{
			addLog('Error while pushing data...');
        	addLog('Process stopped!!');
        	$j("#failure").show();
		}
    }
    function toggleLogs(){
    	var logtext = $j("#loglink").text();
    	 if (logtext === "Show logs") {
    		 $j("#log").show();
    		 $j("#loglink").text('Hide logs');
         } else if (logtext === "Hide logs") {
        	 $j("#log").hide();
        	 $j("#loglink").text('Show logs');
         }
    }
    function updateStatusOnUi(data) {
        if (data.status === "success") {
            onSuccess({success: true});
        } else if (data.status === "error") {
            onError(data);
        } else {
            setTimeout(checkStatus, 5000);
        }
    }
    function checkStatus() {
        $j.ajax({
            "type": "GET",
            "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/startsyncstatus.form",
            "data": {},
            "dataType": "json",
            "success": updateStatusOnUi,
            "error": onError
        });
    }
    function addLog(str){
    	$j('#log').val(new Date().toLocaleString() +': '+ str +'\\\n'+ $j('#log').val());
    }
    $j(document).ready(function () {
        $j("#progress").hide();
        $j("#success").hide();
        $j("#failure").hide();
    });
</script>

<h2><spring:message code="bahmnisyncworker.sync.process"/></h2>

<br/>

<form>
	<table>
		<tr class="oddRow">
			<th style="text-align: left"><openmrs:message code="bahmnisyncworker.last.sync.date.label"/>:</th>
			<td>${allowUpload}</td>
		</tr>
		<tr class="evenRow">
			<th style="text-align: left"><openmrs:message code="bahmnisyncworker.sync.status"/>:</th>
			<td>${disallowUpload}</td>
		</tr>		
	</table>
</form>

<br/>

<input id="syncButton" type="submit" value='<openmrs:message code="bahmnisyncworker.start.sync.label"/>' onclick="syncProcess()">
<br>
<div id="progress">
    <p><openmrs:message code="bahmnisyncworker.inProgress.message"/></p>
    <img id="sync_progress_img" src="<openmrs:contextPath/>/images/loading.gif"/>
</div>
<br>
<div id="success">
    <p><openmrs:message code="bahmnisyncworker.completed.message"/></p>
</div>
<div class="error" id="failure">
    <p><openmrs:message code="bahmnisyncworker.failure.message"/></p>
</div>

<a href="#" onclick="toggleLogs()" id="loglink">Show logs</a>
<br/>
<textarea id="log" name="log" rows="20" cols="100" style="display:none;" readonly>
Logs!!!
</textarea>

<%@ include file="/WEB-INF/template/footer.jsp" %>
