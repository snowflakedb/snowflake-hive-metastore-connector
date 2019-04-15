package com.snowflake.core.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class that minimizes the number of statements to execute by
 * rewriting them.
 */
public class CommandBatchRewriter
{
  /**
   * Aggregate the individual commands within them by rewriting the batches
   * if necessary. Batches represent a group of statements that should be
   * executed in order.
   * The commands are aggregated are as follows:
   *  - If there are ADD/DROP PARTITION commands on the same table, combine them
   *    into one command
   *  - If there are duplicate commands, combine the commands as one
   * Note: Assumes that the commands can be aggregated safely (commands
   * should be idempotent and the order shouldn't matter). For example,
   * the user should not aggregate something like
   * "create table t" -> "delete table t" -> "create table t", since this would
   * be aggregated as "create table t" -> "delete table t".
   * @param batches an iterator over the batches to rewrite
   * @return the rewritten batches
   */
  public static List<List<String>> rewriteBatches(Iterable<List<String>> batches)
  {
    BatchRewriter<String, String> rewriter = new BatchRewriter<>(ImmutableList.of(
        addPartitionRule,
        dropPartitionRule,
        defaultRule));
    batches.forEach(rewriter::feedBatch);
    return rewriter.outputBatches();
  }

  // Definition for a rule for aggregating ADD PARTITION commands
  private static final AggregationRule<String, String> addPartitionRule =
      new AggregationRule<String, String>()
      {
        private Pattern regex = Pattern.compile(
            "ALTER EXTERNAL TABLE ([^\\s]+) ADD PARTITION([^;]+);");

        @Override
        public boolean isApplicable(String contents)
        {
          // Check if it's an ADD PARTITION command
          Matcher matcher = regex.matcher(contents);
          return matcher.matches();
        }

        @Override
        public String getAggregationKey(String contents)
        {
          // Generate a key that will be the same for other ADD PARTITION
          // commands on the same table
          Matcher matcher = regex.matcher(contents);

          Preconditions.checkState(matcher.matches());
          return String.format("ALTER EXTERNAL TABLE %s ADD PARTITION ...",
                                 matcher.group(1));
        }

        @Override
        public String applyAggregation(String contents1, String contents2)
        {
          if (contents1.equals(contents2))
          {
            // ADD PARTITION is idempotent, so we should combine two identical
            // commands
            return contents1;
          }

          Matcher matcher1 = regex.matcher(contents1);
          Matcher matcher2 = regex.matcher(contents2);

          Preconditions.checkState(matcher1.matches() && matcher2.matches());
          return String.format(
              "ALTER EXTERNAL TABLE %s ADD PARTITION%s, PARTITION %s;",
                matcher1.group(1), // Table name
                matcher1.group(2), // List of partitions
                matcher2.group(2)  // Add more partitions
          );
        }

        @Override
        public boolean shouldStopAggregating(String contents)
        {
          // TODO: Make configurable
          return contents.length() > 50000;
        }
      };

  // Definition for a rule for aggregating DROP PARTITION commands
  private static final AggregationRule<String, String> dropPartitionRule =
      new AggregationRule<String, String>()
      {
        private Pattern regex = Pattern.compile(
            "ALTER EXTERNAL TABLE ([^\\s]+) DROP PARTITION([^;]+);");

        @Override
        public boolean isApplicable(String contents)
        {
          // Check if it's a DROP PARTITION command
          Matcher matcher = regex.matcher(contents);
          return matcher.matches();
        }

        @Override
        public String getAggregationKey(String contents)
        {
          // Generate a key that will be the same for other DROP PARTITION
          // commands on the same table
          Matcher matcher = regex.matcher(contents);

          Preconditions.checkState(matcher.matches());
          return String.format("ALTER EXTERNAL TABLE %s DROP PARTITION ...",
                               matcher.group(1));
        }

        @Override
        public String applyAggregation(String contents1, String contents2)
        {
          if (contents1.equals(contents2))
          {
            // DROP PARTITION is idempotent, so we should combine two identical
            // commands
            return contents1;
          }

          Matcher matcher1 = regex.matcher(contents1);
          Matcher matcher2 = regex.matcher(contents2);

          Preconditions.checkState(matcher1.matches() && matcher2.matches());
          return String.format(
              "ALTER EXTERNAL TABLE %s DROP PARTITION%s, %s;",
              matcher1.group(1), // Table name
              matcher1.group(2), // List of partitions
              matcher2.group(2)  // Add more partitions
          );
        }

        @Override
        public boolean shouldStopAggregating(String contents)
        {
          // TODO: Make configurable
          return contents.length() > 50000;
        }
      };

  // Definition for a rule for aggregating commands that are identical
  private static final AggregationRule<String, String> defaultRule =
      new AggregationRule<String, String>()
  {
    @Override
    public boolean isApplicable(String contents)
    {
      return true;
    }

    @Override
    public String getAggregationKey(String contents)
    {
      return contents;
    }

    @Override
    public String applyAggregation(String contents1, String contents2)
    {
      return contents1;
    }

    @Override
    public boolean shouldStopAggregating(String contents)
    {
      return false;
    }
  };

  /**
   * Helper class that represents a rule used to aggregate commands
   * When objects are being aggregated, the following occurs:
   *  - Objects are given a unique 'key' derived from their value based on
   *    the aggregation rule
   *  - Objects with the same unique key must be aggregated into a new
   *    combined object, using a method based on the rule
   * @param <E> type of the object being aggregated
   * @param <K> type of the keys used for aggregation
   */
  private static abstract class AggregationRule<E, K>
  {
    /**
     * Whether the rule is applicable for this object or not
     * @param contents the object to aggregate
     * @return whether the object can be aggregated by this rule
     */
    public abstract boolean isApplicable(E contents);

    /**
     * Generates a key from an object to be used for aggregation
     * @param contents the object to aggregate
     * @return the aggregation key
     */
    public abstract K getAggregationKey(E contents);

    /**
     * Generate an aggregate object from two objects
     * @param contents1 an object to aggregate
     * @param contents2 an object to aggregate
     * @return the aggregated object
     */
    public abstract E applyAggregation(E contents1, E contents2);

    /**
     * Determines whether to stop or continue aggregation, based on an
     * aggregated object. If true, aggregation will not be called for the
     * given object
     * @param contents the aggregated object
     * @return whether to stop aggregation or not
     */
    public abstract boolean shouldStopAggregating(E contents);
  }

  /**
   * Class that represents a node in a tree
   * @param <E> The type of content that the node contains
   */
  private static class TreeNode<E>
  {
    // The next nodes in the tree
    private List<TreeNode<E>> children;

    // The data that this node contains
    private E contents;

    /**
     * Constructs a leaf node
     * @param contents the content the new node should contain
     */
    public TreeNode(E contents)
    {
      this(contents, new ArrayList<>());
    }

    /**
     * Constructs a node
     * @param contents the content the new node should contain
     * @param children the child nodes connected to this node
     */
    public TreeNode(E contents, List<TreeNode<E>> children)
    {
      this.contents = contents;
      this.children = children;
    }

    /**
     * Getter for the contents of the node
     * @return the contents of the node
     */
    public E getContents()
    {
      return contents;
    }

    /**
     * Setter for the contents of the node
     * @param contents the contents of the node
     */
    public void setContents(E contents)
    {
      this.contents = contents;
    }

    /**
     * Getter for the next nodes in the tree
     * @return the next nodes in the tree
     */
    public List<TreeNode<E>> getChildren()
    {
      return children;
    }

    /**
     * Helper method that traverses the tree and outputs the contents of each
     * tree in pre-order.
     * @param node the root of the tree to traverse
     * @param <E> the type of content each node contains
     * @return the content of each node in order
     */
    public static <E> List<E> asFlatList(TreeNode<E> node)
    {
      List<E> output = new ArrayList<>();
      output.add(node.contents);
      node.children.stream()
          .map(TreeNode::asFlatList).collect(Collectors.toList())
          .forEach(output::addAll);
      return output;
    }
  }

  /**
   * Helper class that rewrites multiples of batches of objects
   * @param <E> the object being batched
   * @param <K> type of the keys used for aggregation
   */
  private static class BatchRewriter<E, K>
  {
    // The root that represents all batches given to the rewriter
    private TreeNode<E> root;

    // Mapping between a key and the node that the key was derived from
    private Map<K, TreeNode<E>> keyToNodeMap;

    // The aggregation rules used for the rewrite
    private List<AggregationRule<E, K>> rules;

    /**
     * Constructs a new batch rewriter
     * @param rules the aggregation rules used for the rewrite
     */
    public BatchRewriter(List<AggregationRule<E, K>> rules)
    {
      this.keyToNodeMap = new HashMap<>();
      this.rules = rules;
      this.root = new TreeNode<>(null);
    }

    /**
     * Provides a batch to the rewriter to be processed/aggregated with
     * earlier batches
     * @param batch the batch
     */
    public void feedBatch(List<E> batch)
    {
      Preconditions.checkArgument(!batch.isEmpty());

      // Create and link nodes of the batch into a tree
      List<TreeNode<E>> newNodes =
          batch.stream().map(TreeNode::new).collect(Collectors.toList());
      for (int i = 0; i < newNodes.size() - 1; i++)
      {
        newNodes.get(i).children.add(newNodes.get(i + 1));
      }

      feedNode(newNodes.get(0), root);
    }

    /**
     * Helper method that processes/aggregates a tree to be aggregated with
     * other trees
     * @param node the root of the tree to be added
     * @param parentIfUnique the parent node that the tree would be added to,
     *                       if the aggregation key were unique
     */
    private void feedNode(TreeNode<E> node, TreeNode<E> parentIfUnique)
    {
      // Use a stack, since recursion is too expensive
      Stack<Pair<TreeNode<E>, TreeNode<E>>> executionStack = new Stack<>();
      executionStack.push(Pair.of(node, parentIfUnique));

      while (!executionStack.empty())
      {
        node = executionStack.peek().getKey();
        parentIfUnique= executionStack.pop().getValue();

        // Check for key collisions
        AggregationRule<E, K> rule = matchRule(node.getContents());
        K key = rule.getAggregationKey(node.getContents());
        if (!keyToNodeMap.containsKey(key))
        {
          // Create a copy of this node, and add it
          TreeNode<E> nodeCopy = new TreeNode<>(node.getContents());
          parentIfUnique.getChildren().add(nodeCopy);
          keyToNodeMap.put(key, nodeCopy);

          // Recurse on children
          node.getChildren().forEach(child -> executionStack.push(Pair.of(child, nodeCopy)));
        }
        else
        {
          // Aggregate based on a rule
          TreeNode<E> existingNode = keyToNodeMap.get(key);
          existingNode.setContents(rule.applyAggregation(
              existingNode.getContents(),
              node.getContents()));

          if (rule.shouldStopAggregating(existingNode.getContents()))
          {
            keyToNodeMap.remove(key);
          }

          // Recurse on children
          node.getChildren().forEach(child -> executionStack.push(Pair.of(child, existingNode)));
        }
      }
    }

    /**
     * Helper method that determines which rule should apply to the object
     * being aggregated
     * @param contents the object to aggregate
     * @return the first rule that matches the object
     */
    private AggregationRule<E, K> matchRule(E contents)
    {
      Optional<AggregationRule<E, K>> ruleOpt = rules.stream()
          .filter(r -> r.isApplicable(contents))
          .findFirst();
      Preconditions.checkState(ruleOpt.isPresent());
      return ruleOpt.get();
    }

    /**
     * Provides the processed/aggregated batches from the rewriter
     * @return the rewritten batches. Note: the number of batches returned may
     *         be lower than the number of batches provided
     */
    public List<List<E>> outputBatches()
    {
      return root.getChildren()
          .stream().map(TreeNode::asFlatList).collect(Collectors.toList());
    }
  }
}
