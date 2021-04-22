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
    	
    	$j("#syncButton").prop("disabled", true);
    	$j("#progress").hide();
        $j("#success").hide();
        $j("#failure").hide();
    	$j('#log').val('');
    	
    	isSyncReady();
    	
    }
    function isSyncReady(){
		addLog('Checking Bahmnisync global properties...');
		$j("#progress").show();
		
    	$j.ajax({
    		"type": "GET",
            "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/syncready.form",
            "data": {},
            "dataType": "json",
            "success": logSyncReady,
            "error": onError
        });
    }
    
    function logSyncReady(data){
    	if (data.ready === true) {
    		addLog('Global Properties are available...');
    		checkKafka(data);
    	} else{
        	addLog('Some or all Global Properties missing...');
        	addLog('Process stopped!!');
        	 $j("#progress").hide();
             $j("#success").hide();
             $j("#failure").show();
             $j("#syncButton").prop("disabled", false);
        }
    }
    
    function checkKafka(data){
   		addLog('Checking Connection with Kafka...');
   		$j.ajax({
               "type": "GET",
               "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/kafkaconnection.form",
               "data": {},
               "dataType": "json",
               "success": checkKafkaLog,
               "error": onError
           });
      
    } 
    
    function checkKafkaLog(data){
    	if (data.ready === true) {
    		checkConnection(data);
    	} else{
        	addLog('Unable to connect to Kafka server...');
        	addLog('Process stopped!!');
        	 $j("#progress").hide();
             $j("#success").hide();
             $j("#failure").show();
             $j("#syncButton").prop("disabled", false);
        }
    }
    
    function checkConnection(data){
    					
   		addLog('Checking Connection to Master...');
   		$j.ajax({
               "type": "GET",
               "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/checkconnection.form",
               "data": {},
               "dataType": "json",
               "success": pushData,
               "error": onError
           });
      
    } 
    
	function pushData(data){
		if (data.ready === true) {
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
			addLog('Can not connection to Master node...');
        	addLog('Process stopped!!');
        	$j("#progress").hide();
            $j("#success").hide();
            $j("#failure").show();
            $j("#syncButton").prop("disabled", false);
		}
    }
	function pullData(data){
		if (data.ready === true) {
			addLog('Data Push completed...');
			
			addLog('Starting data pull from Master...');
			$j.ajax({
                "type": "GET",
                "url": "${pageContext.request.contextPath}/module/bahmnisyncworker/startPullData.form",
                "data": {},
                "dataType": "json",
                "success": rebuildIndex,
                "error": onError
            });
		} else{
			addLog('Error while pushing data...');
        	addLog('Process stopped!!');
        	$j("#progress").hide();
            $j("#success").hide();
            $j("#failure").show();
            $j("#syncButton").prop("disabled", false);
		}
    }
	
	function rebuildIndex(data){
		if (data.ready === true) {
			addLog('Data Pull completed...');
			addLog('Rebuilding index...!!');
			$j.ajax({
	            "type": "POST",
	            "url": "${pageContext.request.contextPath}/admin/maintenance/rebuildSearchIndex.htm",
	            "data": {},
	            "dataType": "json",
	            "success": complete,
	            "error": onError
	        });
			
		} else{
			addLog('Error while pulling data...');
        	addLog('Process stopped!!');
        	$j("#progress").hide();
            $j("#success").hide();
            $j("#failure").show();
            $j("#syncButton").prop("disabled", false);
		}
	}
	 
	 function checkStatus() {
	        $j.ajax({
	            "type": "GET",
	            "url": "${pageContext.request.contextPath}/admin/maintenance/rebuildSearchIndexStatus.htm",
	            "data": {},
	            "dataType": "json",
	            "success": complete,
	            "error": onError
	        });
	    } 
	

	function complete(data){
		if (data.status === "success") {
			addLog('Index Resbuild completed...');
			addLog('Process completed!!');
			$j("#success").show();
			$j("#progress").hide();
			$j("#failure").hide();
			$j("#syncButton").prop("disabled", false);
			
		} else if (data.status === "error") {
			addLog('Error while rebuilding index...');
        	addLog('Process stopped!!');
        	$j("#progress").hide();
            $j("#success").hide();
            $j("#failure").show();
            $j("#syncButton").prop("disabled", false);
		} else {
            setTimeout(checkStatus, 5000);
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
        
        var value = "${syncMessage}";
        if(value == '')
        	$j("#message_row").hide();
        else
        	$j("#message_row").show();
        
    });
</script>

<h2>Sync Process</h2>

<br/>

<form>
	<table>
		<tr class="oddRow">
			<th style="text-align: left">Last known sync date:</th>
			<td id="syncDate">${syncDate}</td>
		</tr>
		<tr class="evenRow">
			<th style="text-align: left">Last Sync status:</th>
			<td d="syncStatus">${syncStatus}</td>
		</tr>	
		<tr class="oddRow" id="message_row">
			<th style="text-align: left">Error Message:</th>
			<td d="syncMessage">${syncMessage}</td>
		</tr>	
	</table>
</form>

<br/>

<input id="syncButton" type="submit" value='Start Sync Process' onclick="syncProcess()">
<br>
<div id="progress">
    <p>Inprogress</p>
    <img id="sync_progress_img" src="<openmrs:contextPath/>/images/loading.gif"/>
</div>
<br>
<div id="success">
    <p>Success</p>
</div>
<div class="error" id="failure">
    <p>Failure</p>
</div>

</br>
<a href="#" onclick="toggleLogs()" id="loglink">Show logs</a>
<br/>
<textarea id="log" name="log" rows="20" cols="100" style="display:none;" readonly>
Logs!!!
</textarea>

<%@ include file="/WEB-INF/template/footer.jsp" %>
