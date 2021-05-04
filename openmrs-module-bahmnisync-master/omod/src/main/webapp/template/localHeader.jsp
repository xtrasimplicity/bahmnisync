<%--
  The contents of this file are subject to the OpenMRS Public License
  Version 1.0 (the "License"); you may not use this file except in
  compliance with the License. You may obtain a copy of the License at
  http://license.openmrs.org
  Software distributed under the License is distributed on an "AS IS"
  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
  License for the specific language governing rights and limitations
  under the License.
  Copyright (C) OpenMRS, LLC.  All Rights Reserved.
  --%>

<spring:htmlEscape defaultHtmlEscape="true"/>
<ul id="menu">
	<li class="first"><a
			href="${pageContext.request.contextPath}/admin"><spring:message
			code="admin.title.short"/></a></li>

	<li
			<c:if test='<%= request.getRequestURI().contains("/config") %>'>class="active"</c:if>>
		<a
				href="${pageContext.request.contextPath}/module/bahmnisyncmaster/config.form">Config</a>
	</li>
	
	<li
			<c:if test='<%= request.getRequestURI().contains("/conflict") %>'>class="active"</c:if>>
		<a
				href="${pageContext.request.contextPath}/module/bahmnisyncmaster/conflict2.form">Conflicts</a>
	</li>
	
	<li
			<c:if test='<%= request.getRequestURI().contains("/error") %>'>class="active"</c:if>>
		<a
				href="${pageContext.request.contextPath}/module/bahmnisyncmaster/errors.form">Errors</a>
	</li>
	
	<li
			<c:if test='<%= request.getRequestURI().contains("/log") %>'>class="active"</c:if>>
		<a
				href="${pageContext.request.contextPath}/module/bahmnisyncmaster/log.form">Logs</a>
	</li>

</ul>