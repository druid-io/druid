/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.math.expr;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.antlr.ExprLexer;
import org.apache.druid.math.expr.antlr.ExprParser;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);
  private static final Map<String, Function> FUNCTIONS;
  private static final Map<String, ApplyFunction> APPLY_FUNCTIONS;

  static {
    Map<String, Function> functionMap = new HashMap<>();
    for (Class clazz : Function.class.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && Function.class.isAssignableFrom(clazz)) {
        try {
          Function function = (Function) clazz.newInstance();
          functionMap.put(StringUtils.toLowerCase(function.name()), function);
        }
        catch (Exception e) {
          log.info("failed to instantiate " + clazz.getName() + ".. ignoring", e);
        }
      }
    }
    FUNCTIONS = ImmutableMap.copyOf(functionMap);

    Map<String, ApplyFunction> applyFunctionMap = new HashMap<>();
    for (Class clazz : ApplyFunction.class.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && ApplyFunction.class.isAssignableFrom(clazz)) {
        try {
          ApplyFunction function = (ApplyFunction) clazz.newInstance();
          applyFunctionMap.put(StringUtils.toLowerCase(function.name()), function);
        }
        catch (Exception e) {
          log.info("failed to instantiate " + clazz.getName() + ".. ignoring", e);
        }
      }
    }
    APPLY_FUNCTIONS = ImmutableMap.copyOf(applyFunctionMap);
  }

  public static Function getFunction(String name)
  {
    return FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  public static ApplyFunction getApplyFunction(String name)
  {
    return APPLY_FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  public static Expr parse(String in, ExprMacroTable macroTable)
  {
    return parse(in, macroTable, true);
  }

  @VisibleForTesting
  static Expr parse(String in, ExprMacroTable macroTable, boolean withFlatten)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    ParseTree parseTree = parser.expr();
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, macroTable);
    walker.walk(listener, parseTree);
    return withFlatten ? flatten(listener.getAST()) : listener.getAST();
  }

  public static Expr flatten(Expr expr)
  {
    if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      Expr left = flatten(binary.left);
      Expr right = flatten(binary.right);
      if (Evals.isAllConstants(left, right)) {
        expr = expr.eval(null).toExpr();
      } else if (left != binary.left || right != binary.right) {
        return Evals.binaryOp(binary, left, right);
      }
    } else if (expr instanceof UnaryExpr) {
      UnaryExpr unary = (UnaryExpr) expr;
      Expr eval = flatten(unary.expr);
      if (eval instanceof ConstantExpr) {
        expr = expr.eval(null).toExpr();
      } else if (eval != unary.expr) {
        if (expr instanceof UnaryMinusExpr) {
          expr = new UnaryMinusExpr(eval);
        } else if (expr instanceof UnaryNotExpr) {
          expr = new UnaryNotExpr(eval);
        } else {
          expr = unary; // unknown type..
        }
      }
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      List<Expr> args = functionExpr.args;
      boolean flattened = false;
      List<Expr> flattening = Lists.newArrayListWithCapacity(args.size());
      for (Expr arg : args) {
        Expr flatten = flatten(arg);
        flattened |= flatten != arg;
        flattening.add(flatten);
      }
      if (Evals.isAllConstants(flattening)) {
        expr = expr.eval(null).toExpr();
      } else if (flattened) {
        expr = new FunctionExpr(functionExpr.function, functionExpr.name, flattening);
      }
    }
    return expr;
  }

  public static Expr applyUnappliedIdentifiers(Expr expr, List<String> unapplied)
  {
    Preconditions.checkArgument(unapplied.size() > 0);

    ApplyFunction fn;
    final LambdaExpr lambdaExpr;
    final List<Expr> args;

    // any unapplied identifiers that are inside a lambda expression need that lambda expression to be rewritten
    Expr newExpr = expr.visit(
        new Expr.Shuttle()
        {
          @Override
          public Expr visit(Expr expr)
          {
            if (expr instanceof ApplyFunctionExpr) {
              // try to lift unapplied arguments into the apply function lambda
              return liftApplyLambda((ApplyFunctionExpr) expr, unapplied);
            } else if (expr instanceof FunctionExpr && ((FunctionExpr) expr).function instanceof Function.ArrayFunction) {
              // check array function arguments for unapplied identifiers to transform if necessary
              FunctionExpr fnExpr = (FunctionExpr) expr;
              Function.ArrayFunction arrayFn = (Function.ArrayFunction) fnExpr.function;
              Set<Expr> arrayInputs = arrayFn.getArrayInputs(fnExpr.args);
              List<Expr> newArgs = new ArrayList<>();
              for (Expr arg : fnExpr.args) {
                if (arg.getIdentifierIfIdentifier() == null && arrayInputs.contains(arg)) {
                  Expr newArg = applyUnappliedIdentifiers(arg, unapplied);
                  newArgs.add(newArg);
                } else {
                  newArgs.add(arg);
                }
              }

              FunctionExpr newFnExpr = new FunctionExpr(arrayFn, arrayFn.name(), newArgs);
              return newFnExpr;
            }
            return expr;
          }
        }
    );

    Expr.BindingDetails newExprBindings = newExpr.analyzeInputs();
    final Set<String> expectedArrays = newExprBindings.getArrayVariables();
    List<String> remainingUnappliedArgs =
        unapplied.stream().filter(x -> !expectedArrays.contains(x)).collect(Collectors.toList());

    // if lifting the lambdas got rid of all missing bindings, return the transformed expression
    if (remainingUnappliedArgs.size() == 0) {
      return newExpr;
    }

    // else, it *should be safe* to wrap in either map or cartesian_map because we still have missing bindings that
    // were *not* referenced in a lambda body
    if (remainingUnappliedArgs.size() == 1) {
      fn = new ApplyFunction.MapFunction();
      IdentifierExpr lambdaArg = new IdentifierExpr(remainingUnappliedArgs.iterator().next());
      lambdaExpr = new LambdaExpr(ImmutableList.of(lambdaArg), expr);
      args = ImmutableList.of(lambdaArg);
    } else {
      fn = new ApplyFunction.CartesianMapFunction();
      List<IdentifierExpr> identifiers = new ArrayList<>(remainingUnappliedArgs.size());
      args = new ArrayList<>(remainingUnappliedArgs.size());
      for (String remainingUnappliedArg : remainingUnappliedArgs) {
        IdentifierExpr arg = new IdentifierExpr(remainingUnappliedArg);
        identifiers.add(arg);
        args.add(arg);
      }
      lambdaExpr = new LambdaExpr(identifiers, expr);
    }

    Expr magic = new ApplyFunctionExpr(fn, fn.name(), lambdaExpr, args);
    return magic;
  }

  /**
   * Performs partial lifting of free identifiers of the lambda expression of an {@link ApplyFunctionExpr}, constrained
   * by a list of "unapplied" identifiers, and translating them into arguments of a new {@link LambdaExpr} and
   * {@link ApplyFunctionExpr} as appropriate.
   *
   * The "unapplied" identifiers list is used to allow say only lifting array identifiers and adding it to the cartesian
   * product to allow "magical" translation of multi-value string dimensions which are expressed as single value
   * dimensions to function correctly and as expected.
   */
  private static ApplyFunctionExpr liftApplyLambda(ApplyFunctionExpr expr, List<String> unappliedArgs)
  {
    Expr.BindingDetails lambdaBinding = expr.lambdaExpr.analyzeInputs();
    // this will _not_ include the lambda identifiers.. anything in this list needs to be applied
    List<IdentifierExpr> unappliedLambdaBindings = lambdaBinding.getFreeVariables()
                                                         .stream()
                                                         .filter(unappliedArgs::contains)
                                                         .map(IdentifierExpr::new)
                                                         .collect(Collectors.toList());

    if (unappliedLambdaBindings.size() == 0) {
      return expr;
    }

    final ApplyFunction newFn;
    final ApplyFunctionExpr newExpr;

    final List<Expr> newArgs = new ArrayList<>(expr.argsExpr);
    newArgs.addAll(unappliedLambdaBindings);

    switch (expr.function.name()) {
      case ApplyFunction.MapFunction.NAME:
      case ApplyFunction.CartesianMapFunction.NAME:
        // map(x -> x + y, x) => cartesian_map((x, y) -> x + y, x, y)
        // cartesian_map((x, y) -> x + y + z, x, y) => cartesian_map((x, y, z) -> x + y + z, x, y, z)
        final List<IdentifierExpr> lambdaIds = new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedArgs.size());
        lambdaIds.addAll(expr.lambdaExpr.getIdentifierExprs());
        lambdaIds.addAll(unappliedLambdaBindings);
        final LambdaExpr newLambda = new LambdaExpr(lambdaIds, expr.lambdaExpr.getExpr());
        newFn = new ApplyFunction.CartesianMapFunction();
        newExpr = new ApplyFunctionExpr(newFn, newFn.name(), newLambda, newArgs);
        break;
      case ApplyFunction.AllMatchFunction.NAME:
      case ApplyFunction.AnyMatchFunction.NAME:
      case ApplyFunction.FilterFunction.NAME:
        // i'm lazy and didn't add 'cartesian_filter', 'cartesian_any', and 'cartesian_and', so instead steal the match
        // expressions lambda and translate it into a 'cartesian_map', and apply that to the match function with a new
        // identity expression lambda since the input is an array of boolean expression results (or should be..)
        // filter(x -> x > y, x) => filter(x -> x, cartesian_map((x,y) -> x > y, x, y))
        // any(x -> x > y, x) => any(x -> x, cartesian_map((x, y) -> x > y, x, y))
        // all(x -> x > y, x) => all(x -> x, cartesian_map((x, y) -> x > y, x, y))
        ApplyFunction newArrayFn = new ApplyFunction.CartesianMapFunction();
        IdentifierExpr identityExprIdentifier = new IdentifierExpr("_");
        LambdaExpr identityExpr = new LambdaExpr(ImmutableList.of(identityExprIdentifier), identityExprIdentifier);
        ApplyFunctionExpr arrayExpr = new ApplyFunctionExpr(newArrayFn, newArrayFn.name(), identityExpr, newArgs);
        newExpr = new ApplyFunctionExpr(expr.function, expr.function.name(), identityExpr, ImmutableList.of(arrayExpr));
        break;
      case ApplyFunction.FoldFunction.NAME:
      case ApplyFunction.CartesianFoldFunction.NAME:
        // fold((x, acc) -> acc + x + y, x, acc) => cartesian_fold((x, y, acc) -> acc + x + y, x, y, acc)
        // cartesian_fold((x, y, acc) -> acc + x + y + z, x, y, acc) => cartesian_fold((x, y, z, acc) -> acc + x + y + z, x, y, z, acc)

        final List<Expr> newFoldArgs = new ArrayList<>(expr.argsExpr.size() + unappliedArgs.size());
        final List<IdentifierExpr> newFoldLambdaIdentifiers = new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedArgs.size());
        final List<IdentifierExpr> existingFoldLambdaIdentifiers = expr.lambdaExpr.getIdentifierExprs();
        // accumulator argument is last argument, slice it off when constructing new arg list and lambda args identifiers
        for (int i = 0; i < expr.argsExpr.size() - 1; i++) {
          newFoldArgs.add(expr.argsExpr.get(i));
          newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(i));
        }
        newFoldArgs.addAll(unappliedLambdaBindings);
        newFoldLambdaIdentifiers.addAll(unappliedLambdaBindings);
        // add accumulator last
        newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(existingFoldLambdaIdentifiers.size() - 1));
        newArgs.addAll(unappliedLambdaBindings);
        final LambdaExpr newFoldLambda = new LambdaExpr(newFoldLambdaIdentifiers, expr.lambdaExpr.getExpr());

        newFn = new ApplyFunction.CartesianFoldFunction();
        newExpr = new ApplyFunctionExpr(newFn, newFn.name(), newFoldLambda, newFoldArgs);
        break;
      default:
        throw new RE("Unable to transform apply function:[%s]", expr.function.name());
    }

    return newExpr;
  }

  public static void validateExpr(Expr expression, Expr.BindingDetails bindingDetails)
  {
    final Set<String> inconsistentIdentifierUsage =
        Sets.intersection(bindingDetails.getScalarVariables(), bindingDetails.getArrayVariables());
    if (inconsistentIdentifierUsage.size() != 0) {
      throw new RE("Invalid expression: %s; %s used as both scalar and array variables", expression, inconsistentIdentifierUsage);
    }
  }

  public static Expr.ObjectBinding withMap(final Map<String, ?> bindings)
  {
    return bindings::get;
  }

  public static Expr.ObjectBinding withSuppliers(final Map<String, Supplier<Object>> bindings)
  {
    return (String name) -> {
      Supplier<Object> supplier = bindings.get(name);
      return supplier == null ? null : supplier.get();
    };
  }
}
