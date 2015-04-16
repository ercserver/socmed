
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/sql" prefix="sql"%>

<html>
<head>
    <title>SocMed - DB Admin Tools - Select</title>
</head> 
<body>
    <form id="ChooseTable" method="post" action="DBAdminServlet">
        <input type="text" placeholder="TableName" />
        <input type="submit">
    </form>

</body>
</html>