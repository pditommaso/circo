/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
 *
 *    Circo is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Circo is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Circo.  If not, see <http://www.gnu.org/licenses/>.
 */

package circo.utils

import org.codehaus.groovy.ast.*
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.control.messages.SyntaxErrorMessage
import org.codehaus.groovy.syntax.SyntaxException
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation
/**
 * Add the field {@code serialVersionUID} required by the Java serialization
 * mechanism to the class
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@GroovyASTTransformation(phase=CompilePhase.SEMANTIC_ANALYSIS)
public class SerializableASTTransformation implements ASTTransformation {

    static final def random = new Random()

    public void visit(ASTNode[] nodes, SourceUnit source) {
        if (nodes.length != 2 || !(nodes[0] instanceof AnnotationNode) || !(nodes[1] instanceof AnnotatedNode)) {
            addError("Internal error: expecting [AnnotationNode, AnnotatedNode] but got: " + Arrays.asList(nodes), nodes[0], source);
        }

        AnnotatedNode parent = (AnnotatedNode) nodes[1];
        AnnotationNode serializeAnnotation = (AnnotationNode) nodes[0];


        if (parent instanceof ClassNode) {
            ClassNode classNode = (ClassNode) parent;

            /* Add the Serializable interface */
            classNode.addInterface(ClassHelper.make(java.io.Serializable.class));

            /* Add the serialVersionUID  field */
            def modifier = FieldNode.ACC_PRIVATE | FieldNode.ACC_STATIC | FieldNode.ACC_FINAL
            def value = new ConstantExpression( getVersion(serializeAnnotation), true )

            classNode.addField("serialVersionUID", modifier, ClassHelper.make(Long.TYPE), value)
        }
    }

    static private long getVersion( AnnotationNode annotation ) {
        def value = annotation.getMember('value')?.getText();
        def result = value && value.isLong() ? Long.parseLong(value) : -1

        return result
    }

    static public void addError(String msg, ASTNode expr, SourceUnit source) {
        int line = expr.getLineNumber();
        int col = expr.getColumnNumber();
        source.getErrorCollector().addErrorAndContinue(
                new SyntaxErrorMessage(new SyntaxException(msg + '\n', line, col), source)
        );
    }
}