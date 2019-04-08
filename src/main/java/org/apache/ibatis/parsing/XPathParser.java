/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.parsing;

import org.apache.ibatis.builder.BuilderException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.*;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XPathParser {

  /**
   * XML Document 对象
   * XML 文件被解析后，生成的 org.w3c.dom.Document 对象
   */
  private final Document document;
  /**
   * 是否校验
   * 是否校验 XML 文件。一般情况下，值为 true
   */
  private boolean validation;
  /**
   *  XML 实体解析器
   *
   * org.xml.sax.EntityResolver 对象，XML 实体解析器。
   * 默认情况下，对 XML 进行校验时，会基于 XML 文档开始位置指定的 DTD 文件或 XSD 文件。
   * 例如说，解析 mybatis-config.xml 配置文件时，会加载 http://mybatis.org/dtd/mybatis-3-config.dtd 这个 DTD 文件。
   * 但是，如果每个应用启动都从网络加载该 DTD 文件，势必在弱网络下体验非常下，甚至说应用部署在无网络的环境下，
   * 还会导致下载不下来，那么就会出现 XML 校验失败的情况。所以，在实际场景下，MyBatis 自定义了 EntityResolver 的实现，
   * 达到使用本地 DTD 文件，从而避免下载网络 DTD 文件的效果。详细解析见 XMLMapperEntityResolver 。
   */
  private EntityResolver entityResolver;
  /**
   * 变量 Properties 对象
   *
   * 用来替换需要动态配置的属性值，例如 driver、url、username、password 等
   * 具体如何实现的，查看 PropertyParser#parse(String string, Properties variables) 方法
   */
  private Properties variables;
  /**
   * Java XPath 对象
   *
   * javax.xml.xpath.XPath 对象，用于查询 XML 中的节点和元素。
   */
  private XPath xpath;

  public XPathParser(String xml) {
    commonConstructor(false, null, null);
    this.document = createDocument(new InputSource(new StringReader(xml)));
  }

  public XPathParser(Reader reader) {
    commonConstructor(false, null, null);
    this.document = createDocument(new InputSource(reader));
  }

  public XPathParser(InputStream inputStream) {
    commonConstructor(false, null, null);
    this.document = createDocument(new InputSource(inputStream));
  }

  public XPathParser(Document document) {
    commonConstructor(false, null, null);
    this.document = document;
  }

  public XPathParser(String xml, boolean validation) {
    commonConstructor(validation, null, null);
    this.document = createDocument(new InputSource(new StringReader(xml)));
  }

  public XPathParser(Reader reader, boolean validation) {
    commonConstructor(validation, null, null);
    this.document = createDocument(new InputSource(reader));
  }

  public XPathParser(InputStream inputStream, boolean validation) {
    commonConstructor(validation, null, null);
    this.document = createDocument(new InputSource(inputStream));
  }

  public XPathParser(Document document, boolean validation) {
    commonConstructor(validation, null, null);
    this.document = document;
  }

  public XPathParser(String xml, boolean validation, Properties variables) {
    commonConstructor(validation, variables, null);
    this.document = createDocument(new InputSource(new StringReader(xml)));
  }

  public XPathParser(Reader reader, boolean validation, Properties variables) {
    commonConstructor(validation, variables, null);
    this.document = createDocument(new InputSource(reader));
  }

  public XPathParser(InputStream inputStream, boolean validation, Properties variables) {
    commonConstructor(validation, variables, null);
    this.document = createDocument(new InputSource(inputStream));
  }

  public XPathParser(Document document, boolean validation, Properties variables) {
    commonConstructor(validation, variables, null);
    this.document = document;
  }

  public XPathParser(String xml, boolean validation, Properties variables, EntityResolver entityResolver) {
    commonConstructor(validation, variables, entityResolver);
    this.document = createDocument(new InputSource(new StringReader(xml)));
  }

  /**
   * 构造 XPathParser 对象
   * @param reader XML 文件流
   * @param validation 是否校验 XML
   * @param variables Properties 对象
   * @param entityResolver  XML 实体解析器
   */
  public XPathParser(Reader reader, boolean validation, Properties variables, EntityResolver entityResolver) {
    // 公用的构造方法
    commonConstructor(validation, variables, entityResolver);
    // 调用 createDocument(InputSource inputSource) 方法，将 XML 文件解析成 Document 对象
    this.document = createDocument(new InputSource(reader));
  }

  public XPathParser(InputStream inputStream, boolean validation, Properties variables, EntityResolver entityResolver) {
    commonConstructor(validation, variables, entityResolver);
    this.document = createDocument(new InputSource(inputStream));
  }

  public XPathParser(Document document, boolean validation, Properties variables, EntityResolver entityResolver) {
    commonConstructor(validation, variables, entityResolver);
    this.document = document;
  }

  public void setVariables(Properties variables) {
    this.variables = variables;
  }

  public String evalString(String expression) {
    return evalString(document, expression);
  }

  /**
   * 获取节点 String 类型的值
   * @param root 节点
   * @param expression 表达式
   */
  public String evalString(Object root, String expression) {
    // 调用 evaluate(String expression, Object root, QName returnType) 方法，获得值。
    // 其中，returnType 方法传入的是 XPathConstants.STRING ，表示返回的值是 String 类型。
    String result = (String) evaluate(expression, root, XPathConstants.STRING);
    // 调用 PropertyParser#parse(String string, Properties variables) 方法，
    // 基于 variables 替换动态值，如果 result 为动态值，
    // 这就是 MyBatis 如何替换掉 XML 中的动态值实现的方式。
    result = PropertyParser.parse(result, variables);
    return result;
  }

  public Boolean evalBoolean(String expression) {
    return evalBoolean(document, expression);
  }

  public Boolean evalBoolean(Object root, String expression) {
    return (Boolean) evaluate(expression, root, XPathConstants.BOOLEAN);
  }

  public Short evalShort(String expression) {
    return evalShort(document, expression);
  }

  public Short evalShort(Object root, String expression) {
    return Short.valueOf(evalString(root, expression));
  }

  public Integer evalInteger(String expression) {
    return evalInteger(document, expression);
  }

  public Integer evalInteger(Object root, String expression) {
    return Integer.valueOf(evalString(root, expression));
  }

  public Long evalLong(String expression) {
    return evalLong(document, expression);
  }

  public Long evalLong(Object root, String expression) {
    return Long.valueOf(evalString(root, expression));
  }

  public Float evalFloat(String expression) {
    return evalFloat(document, expression);
  }

  public Float evalFloat(Object root, String expression) {
    return Float.valueOf(evalString(root, expression));
  }

  public Double evalDouble(String expression) {
    return evalDouble(document, expression);
  }

  public Double evalDouble(Object root, String expression) {
    return (Double) evaluate(expression, root, XPathConstants.NUMBER);
  }

  public List<XNode> evalNodes(String expression) {
    return evalNodes(document, expression);
  }

  /**
   * 用于获得 Node 类型的节点的值,封装成 XNode 集合
   * @param root 节点
   * @param expression 表达式
   */
  public List<XNode> evalNodes(Object root, String expression) {
    // 保存 XNode 的 List 容器
    List<XNode> xnodes = new ArrayList<>();
    // 获得 Node 数组
    NodeList nodes = (NodeList) evaluate(expression, root, XPathConstants.NODESET);
    // 封装成 XNode 集合
    for (int i = 0; i < nodes.getLength(); i++) {
      xnodes.add(new XNode(this, nodes.item(i), variables));
    }
    return xnodes;
  }

  public XNode evalNode(String expression) {
    return evalNode(document, expression);
  }

  /**
   * 获得 XNode 对象
   * @param root
   * @param expression
   * @return
   */
  public XNode evalNode(Object root, String expression) {
    // 获得 Node 对象
    Node node = (Node) evaluate(expression, root, XPathConstants.NODE);
    if (node == null) {
      return null;
    }
    // 封装成 XNode 对象
    return new XNode(this, node, variables);
  }

  /**
   * 获得指定元素或节点的值
   * @param expression expression 表达式
   * @param root 指定节点
   * @param returnType 返回类型
   * @return 返回指定元素或节点的值
   */
  private Object evaluate(String expression, Object root, QName returnType) {
    try {
      // 调用 xpath 的 evaluate(String expression, Object root, QName returnType) 方法，
      // 获得指定元素或节点的值
      return xpath.evaluate(expression, root, returnType);
    } catch (Exception e) {
      throw new BuilderException("Error evaluating XPath.  Cause: " + e, e);
    }
  }

  /**
   * 创建 Document 对象
   * @param inputSource XML 文件的 InputSource 对象
   * @return Document 对象
   */
  private Document createDocument(InputSource inputSource) {
    // important: this must only be called AFTER common constructor
    // 这只能在 commonConstructor 构造函数之后调用
    try {
      // 创建 DocumentBuilderFactory 对象
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      // 设置是否验证 XML
      factory.setValidating(validation);

      factory.setNamespaceAware(false);
      factory.setIgnoringComments(true);
      factory.setIgnoringElementContentWhitespace(false);
      factory.setCoalescing(false);
      factory.setExpandEntityReferences(true);

      // 借助 DocumentBuilderFactory 对象创建 DocumentBuilder 对象
      // 具体实现类是 DocumentBuilderImpl ，它继承 DocumentBuilder 抽象类
      DocumentBuilder builder = factory.newDocumentBuilder();
      // 设置实体解析器，即为 XMLMapperEntityResolver 对象
      builder.setEntityResolver(entityResolver);
      // 设置错误处理类 ErrorHandler ，默认实现
      builder.setErrorHandler(new ErrorHandler() {
        @Override
        public void error(SAXParseException exception) throws SAXException {
          throw exception;
        }

        @Override
        public void fatalError(SAXParseException exception) throws SAXException {
          throw exception;
        }

        @Override
        public void warning(SAXParseException exception) throws SAXException {
        }
      });
      // 关键： 解析 XML 文件，即简单的 Java XML API 的使用
      return builder.parse(inputSource);
    } catch (Exception e) {
      throw new BuilderException("Error creating document instance.  Cause: " + e, e);
    }
  }

  private void commonConstructor(boolean validation, Properties variables, EntityResolver entityResolver) {
    this.validation = validation;
    this.entityResolver = entityResolver;
    this.variables = variables;
    // 创建 XPathFactory 对象,用来创建 XPath 对象
    XPathFactory factory = XPathFactory.newInstance();
    this.xpath = factory.newXPath();
  }

}
